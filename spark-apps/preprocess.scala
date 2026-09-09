// ===========================================================================
// PHASE 4 -- Distributed preprocessing in Scala on Spark, managed by YARN
//
// Reads   : /fraudlens/dataset/unprocessed        (28.1 GB raw CSV, 5 chunks)
// Writes  : /fraudlens/dataset/preprocessed       (Parquet, partitioned)
//           /fraudlens/dataset/preprocessed_sample_csv  (20k readable rows)
//
// DESIGN DECISIONS THAT MATTER FOR THE REPORT
//
// 1. EXPLICIT SCHEMA, NEVER inferSchema.
//    inferSchema triggers an extra full pass over 28 GB before any work
//    starts. Declaring the schema costs nothing and halves the I/O.
//
// 2. IMPUTE, DO NOT DROP.
//    The review-1 pipeline used .na.drop(), which silently discarded every
//    dirty row. Here nulls are coalesced to sentinels, so:
//        rows in == rows out == 132,500,000
//    That equality is the number to defend. Byte size will FALL (roughly
//    28 GB -> 7 GB) because Parquet + Snappy is columnar and compressed --
//    that is compression working, not data loss.
//
// 3. REAL CLEANUP WORK ON REAL MESS.
//    - amount arrives as "$1,204.55" / "-$50.00"  -> regex strip, cast double
//    - is_fraud arrives as "Yes"/"No"             -> cast to 0/1
//    - zip arrives as "4917.0" (float, leading zero lost) -> pad to 5 digits
//    - device_metadata is a nested JSON blob, sometimes missing sub-objects
//      -> from_json against a declared schema, null-safe
//
// 4. BROADCAST JOIN.
//    zip_coords (27,321 rows, ~800 KB) is broadcast against 132.5M rows to
//    attach merchant coordinates. This is the performance-tuning
//    demonstration, and it enables the distance feature below.
//
// 5. FEATURES THAT ACTUALLY CARRY SIGNAL.
//    hour, is_weekend-ish day fields, amount_log, is_online, and
//    device_merchant_distance_km (how far the device was from the merchant).
//
// Run:  bash scripts/run_preprocess.sh
// ===========================================================================

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
val HDFS       = "hdfs://namenode:9000"
val INPUT      = HDFS + "/fraudlens/dataset/unprocessed"
val OUTPUT     = HDFS + "/fraudlens/dataset/preprocessed"
val SAMPLE_OUT = HDFS + "/fraudlens/dataset/preprocessed_sample_csv"
val ZIP_LOOKUP = HDFS + "/fraudlens/lookup/zip_coords.csv"

// Number of shuffle partitions used when laying out the partitioned output.
// Without this repartition, 225 input tasks x N partition directories would
// produce thousands of tiny Parquet files.
val OUTPUT_PARTITIONS = 120

spark.conf.set("spark.sql.shuffle.partitions", OUTPUT_PARTITIONS.toString)
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
// Legacy rebase keeps old dates (this dataset starts in the 1990s) writable.
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

println("=" * 74)
println(" PHASE 4: Scala preprocessing on Spark / YARN")
println("=" * 74)
println(s" input  : $INPUT")
println(s" output : $OUTPUT")
println("")

val t0 = System.currentTimeMillis()

// ---------------------------------------------------------------------------
// 1. Declared schema for the raw CSV -- every field read as String, because
//    the raw values are dirty and must be cleaned before casting.
// ---------------------------------------------------------------------------
val rawSchema = StructType(Array(
  StructField("user",            StringType, true),
  StructField("card",            StringType, true),
  StructField("year",            StringType, true),
  StructField("month",           StringType, true),
  StructField("day",             StringType, true),
  StructField("time",            StringType, true),
  StructField("amount",          StringType, true),
  StructField("use_chip",        StringType, true),
  StructField("merchant_name",   StringType, true),
  StructField("merchant_city",   StringType, true),
  StructField("merchant_state",  StringType, true),
  StructField("zip",             StringType, true),
  StructField("mcc",             StringType, true),
  StructField("errors",          StringType, true),
  StructField("is_fraud",        StringType, true),
  StructField("device_metadata", StringType, true)
))

// ---------------------------------------------------------------------------
// 2. Declared schema for the nested JSON. Declaring it (rather than letting
//    Spark guess) is what makes missing sub-objects come back as null
//    instead of throwing.
// ---------------------------------------------------------------------------
val deviceSchema = StructType(Array(
  StructField("network", StructType(Array(
    StructField("vpn", BooleanType, true),
    StructField("ip",  StringType,  true)
  )), true),
  StructField("geo", StructType(Array(
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true)
  )), true),
  StructField("hardware", StructType(Array(
    StructField("os", StringType, true)
  )), true)
))

val rawDf = spark.read
  .schema(rawSchema)
  .option("header", "true")
  .option("quote", "\"")
  .option("escape", "\"")
  .option("multiLine", "false")
  .option("mode", "PERMISSIVE")
  .csv(INPUT)

println(">>> Counting raw input rows (this is the number the output must match)")
val rawCount = rawDf.count()
println(f">>> RAW ROWS IN: $rawCount%,d")
println("")

// ---------------------------------------------------------------------------
// 3. Clean and cast. Nothing is dropped anywhere in this stage.
// ---------------------------------------------------------------------------
val parsed = rawDf
  // --- amount: "$1,204.55" / "-$50.00" -> 1204.55 / -50.0 ------------------
  .withColumn("amount_clean",
    coalesce(
      regexp_replace(trim(col("amount")), "[$,]", "").cast(DoubleType),
      lit(0.0)))
  // --- is_fraud: "Yes"/"No" -> 1/0 -----------------------------------------
  .withColumn("is_fraud_int",
    when(lower(trim(col("is_fraud"))).isin("yes", "1", "true"), lit(1))
      .otherwise(lit(0)))
  // --- numeric ids and date parts, null-safe -------------------------------
  .withColumn("user_id",  coalesce(col("user").cast(IntegerType),  lit(-1)))
  .withColumn("card_id",  coalesce(col("card").cast(IntegerType),  lit(-1)))
  .withColumn("txn_year", coalesce(col("year").cast(IntegerType),  lit(0)))
  .withColumn("txn_month",coalesce(col("month").cast(IntegerType), lit(0)))
  .withColumn("txn_day",  coalesce(col("day").cast(IntegerType),   lit(0)))
  .withColumn("mcc_code", coalesce(col("mcc").cast(IntegerType),   lit(-1)))
  // --- time "13:20" -> hour / minute --------------------------------------
  .withColumn("txn_hour",
    coalesce(split(coalesce(col("time"), lit("00:00")), ":").getItem(0)
      .cast(IntegerType), lit(0)))
  .withColumn("txn_minute",
    coalesce(split(coalesce(col("time"), lit("00:00")), ":").getItem(1)
      .cast(IntegerType), lit(0)))
  // --- categorical imputation: sentinel, never a dropped row ---------------
  .withColumn("merchant_city_clean",
    when(col("merchant_city").isNull || (trim(col("merchant_city")) === ""),
      lit("UNKNOWN")).otherwise(upper(trim(col("merchant_city")))))
  .withColumn("merchant_state_clean",
    when(col("merchant_state").isNull || (trim(col("merchant_state")) === ""),
      lit("UNKNOWN")).otherwise(upper(trim(col("merchant_state")))))
  .withColumn("use_chip_clean",
    when(col("use_chip").isNull || (trim(col("use_chip")) === ""),
      lit("UNKNOWN")).otherwise(trim(col("use_chip"))))
  // --- zip: "4917.0" -> "04917" (the source lost leading zeros) -----------
  .withColumn("zip_clean",
    when(col("zip").isNull || (trim(col("zip")) === ""), lit("00000"))
      .otherwise(lpad(split(trim(col("zip")), "\\.").getItem(0), 5, "0")))
  // --- errors: presence is the signal, not the text ------------------------
  .withColumn("error_flag",
    when(col("errors").isNull || (trim(col("errors")) === ""), lit(0))
      .otherwise(lit(1)))
  .withColumn("error_type",
    when(col("errors").isNull || (trim(col("errors")) === ""),
      lit("NONE")).otherwise(trim(col("errors"))))
  // --- nested JSON --------------------------------------------------------
  .withColumn("dev", from_json(col("device_metadata"), deviceSchema))
  .withColumn("vpn_flag",
    when(col("dev.network.vpn") === true, lit(1)).otherwise(lit(0)))
  .withColumn("device_ip", coalesce(col("dev.network.ip"), lit("0.0.0.0")))
  .withColumn("device_os", coalesce(col("dev.hardware.os"), lit("UNKNOWN")))
  .withColumn("device_lat", col("dev.geo.lat"))
  .withColumn("device_lon", col("dev.geo.lon"))
  // Track WHICH rows had the sub-object missing -- a real feature, and the
  // honest way to record imputation instead of hiding it.
  .withColumn("geo_missing",  when(col("dev.geo").isNull, lit(1)).otherwise(lit(0)))
  .withColumn("hw_missing",   when(col("dev.hardware").isNull, lit(1)).otherwise(lit(0)))
  .drop("dev")

// ---------------------------------------------------------------------------
// 4. BROADCAST JOIN: attach merchant coordinates from the small lookup table.
// ---------------------------------------------------------------------------
val zipSchema = StructType(Array(
  StructField("zip",       StringType, true),
  StructField("state",     StringType, true),
  StructField("latitude",  DoubleType, true),
  StructField("longitude", DoubleType, true)
))

val zipDf = spark.read
  .schema(zipSchema)
  .option("header", "true")
  .csv(ZIP_LOOKUP)
  .select(
    lpad(col("zip"), 5, "0").as("lookup_zip"),
    col("latitude").as("merchant_lat"),
    col("longitude").as("merchant_lon"))
  .dropDuplicates("lookup_zip")

val zipCount = zipDf.count()
println(f">>> Broadcast lookup table: $zipCount%,d ZIP entries")

val joined = parsed.join(
  broadcast(zipDf),
  parsed("zip_clean") === zipDf("lookup_zip"),
  "left"
).drop("lookup_zip")

// ---------------------------------------------------------------------------
// 5. Derived features
// ---------------------------------------------------------------------------
// Haversine great-circle distance, in km, between device and merchant.
val R = 6371.0
val dLat = radians(col("merchant_lat") - col("device_lat"))
val dLon = radians(col("merchant_lon") - col("device_lon"))
val hav  = pow(sin(dLat / lit(2.0)), 2.0) +
           cos(radians(col("device_lat"))) * cos(radians(col("merchant_lat"))) *
           pow(sin(dLon / lit(2.0)), 2.0)

val featured = joined
  .withColumn("amount_abs", abs(col("amount_clean")))
  .withColumn("amount_log", log(lit(1.0) + abs(col("amount_clean"))))
  .withColumn("is_refund", when(col("amount_clean") < 0, lit(1)).otherwise(lit(0)))
  .withColumn("is_online",
    when(col("merchant_city_clean") === "ONLINE", lit(1)).otherwise(lit(0)))
  .withColumn("is_foreign_or_unknown",
    when(length(col("merchant_state_clean")) =!= 2, lit(1)).otherwise(lit(0)))
  .withColumn("is_night",
    when(col("txn_hour") < 6 || col("txn_hour") >= 22, lit(1)).otherwise(lit(0)))
  .withColumn("device_merchant_distance_km",
    coalesce(round(lit(2.0 * R) * asin(sqrt(hav)), 2), lit(-1.0)))
  .withColumn("device_lat_filled", coalesce(col("device_lat"), lit(0.0)))
  .withColumn("device_lon_filled", coalesce(col("device_lon"), lit(0.0)))
  .withColumn("merchant_lat_filled", coalesce(col("merchant_lat"), lit(0.0)))
  .withColumn("merchant_lon_filled", coalesce(col("merchant_lon"), lit(0.0)))
  .withColumn("processed_at", current_timestamp())

// ---------------------------------------------------------------------------
// 6. Final projection -- explicit column list, stable for Phase 5
// ---------------------------------------------------------------------------
val finalDf = featured.select(
  col("user_id").as("user"),
  col("card_id").as("card"),
  col("txn_year").as("year"),
  col("txn_month").as("month"),
  col("txn_day").as("day"),
  col("txn_hour").as("hour"),
  col("txn_minute").as("minute"),
  col("amount_clean").as("amount"),
  col("amount_abs"),
  col("amount_log"),
  col("is_refund"),
  col("use_chip_clean").as("use_chip"),
  col("merchant_name"),
  col("merchant_city_clean").as("merchant_city"),
  col("merchant_state_clean").as("merchant_state"),
  col("zip_clean").as("zip"),
  col("mcc_code").as("mcc"),
  col("error_flag"),
  col("error_type"),
  col("vpn_flag"),
  col("device_ip"),
  col("device_os"),
  col("device_lat_filled").as("device_lat"),
  col("device_lon_filled").as("device_lon"),
  col("geo_missing"),
  col("hw_missing"),
  col("merchant_lat_filled").as("merchant_lat"),
  col("merchant_lon_filled").as("merchant_lon"),
  col("device_merchant_distance_km"),
  col("is_online"),
  col("is_foreign_or_unknown"),
  col("is_night"),
  col("is_fraud_int").as("is_fraud"),
  col("processed_at")
)

// ---------------------------------------------------------------------------
// 7. Write: Parquet, Snappy, partitioned by year/month.
//    The repartition matches the write layout so each partition directory
//    gets a small number of right-sized files instead of hundreds of tiny
//    ones. This is a deliberate performance-tuning step.
// ---------------------------------------------------------------------------
println("")
println(">>> Writing Parquet, partitioned by year/month (Snappy)")

finalDf
  .repartition(OUTPUT_PARTITIONS, col("year"), col("month"))
  .write
  .mode("overwrite")
  .partitionBy("year", "month")
  .parquet(OUTPUT)

val tWrite = System.currentTimeMillis()
println(f">>> Write complete in ${(tWrite - t0) / 1000.0}%.1f s")

// ---------------------------------------------------------------------------
// 8. Verification: rows in must equal rows out
// ---------------------------------------------------------------------------
println("")
println(">>> Verifying")
val outDf    = spark.read.parquet(OUTPUT)
val outCount = outDf.count()
val fraudOut = outDf.filter(col("is_fraud") === 1).count()

println("=" * 74)
println(" ROW-COUNT RECONCILIATION")
println("=" * 74)
println(f" rows in  : $rawCount%,d")
println(f" rows out : $outCount%,d")
println(f" dropped  : ${rawCount - outCount}%,d")
if (rawCount == outCount) {
  println(" RESULT   : PASS -- no rows lost, every dirty value imputed")
} else {
  println(" RESULT   : MISMATCH -- investigate before continuing to Phase 5")
}
println(f" fraud    : $fraudOut%,d (${100.0 * fraudOut / outCount}%.4f%%)")
println("")

// Sanity checks on the cleanup itself.
println(">>> Cleanup spot-checks")
val nullAmounts = outDf.filter(col("amount").isNull).count()
val nullStates  = outDf.filter(col("merchant_state").isNull).count()
val badZips     = outDf.filter(length(col("zip")) =!= 5).count()
val geoMissing  = outDf.filter(col("geo_missing") === 1).count()
val hwMissing   = outDf.filter(col("hw_missing") === 1).count()
val unknownSt   = outDf.filter(col("merchant_state") === "UNKNOWN").count()
println(f" null amounts after cleanup : $nullAmounts%,d  (expect 0)")
println(f" null states  after cleanup : $nullStates%,d  (expect 0)")
println(f" malformed zips             : $badZips%,d  (expect 0)")
println(f" rows imputed for geo       : $geoMissing%,d")
println(f" rows imputed for hardware  : $hwMissing%,d")
println(f" merchant_state = UNKNOWN   : $unknownSt%,d")
println("")

println(">>> Schema of the preprocessed dataset")
outDf.printSchema()

println(">>> Sample rows")
outDf.select("user", "amount", "merchant_state", "zip", "mcc", "hour",
             "vpn_flag", "device_os", "device_merchant_distance_km", "is_fraud")
  .show(10, false)

// ---------------------------------------------------------------------------
// 9. A small CSV sample. Parquet is binary, so this is what you open when
//    someone asks to SEE the cleaned data.
// ---------------------------------------------------------------------------
println(">>> Writing a 20,000-row CSV sample for inspection")
outDf.limit(20000)
  .coalesce(1)
  .write.mode("overwrite")
  .option("header", "true")
  .csv(SAMPLE_OUT)

val elapsed = (System.currentTimeMillis() - t0) / 1000.0
println("")
println("=" * 74)
println(f" PHASE 4 COMPLETE in $elapsed%.1f s (${elapsed / 60.0}%.1f min)")
println("=" * 74)
println(" Next: bash scripts/run_spark_agg.sh   (MapReduce vs Spark benchmark)")
println("       then Phase 5 model training")

System.exit(0)
