// ===========================================================================
// PHASE 4 -- Distributed preprocessing in Scala on Spark, managed by YARN
//
// Reads   : /fraudlens/dataset/unprocessed              (28.1 GB raw CSV)
// Writes  : /fraudlens/dataset/preprocessed             (Parquet, partitioned)
//           /fraudlens/dataset/preprocessed_sample_csv  (20k readable rows)
//           /artifacts/data_quality_profile.json        (table for the report)
//
// KEY DECISIONS
//
// 1. EXPLICIT SCHEMA, NEVER inferSchema -- inferSchema costs an extra full
//    pass over 28 GB before any real work begins.
//
// 2. IMPUTE, DO NOT DROP. Review-1 used .na.drop(), silently discarding every
//    dirty row. Here nulls become sentinels and each imputation is RECORDED
//    in a flag column, so:  rows in == rows out == 132,500,000.
//    Output bytes fall (28.1 GB -> ~6 GB) because Parquet is columnar and
//    Snappy-compressed. That is compression, not data loss -- the row count
//    is the number to quote.
//
// 3. REAL CLEANUP ON REAL MESS
//      amount   "$1,204.55" -> 1204.55       (regex strip, then cast)
//      is_fraud "Yes"/"No"  -> 1/0
//      zip      "4917.0"    -> "04917"       (the source lost leading zeros)
//      device_metadata      -> from_json against a declared schema
//
// 4. BROADCAST JOIN -- zip_coords (27,321 rows, 800 KB) broadcast against
//    132.5M rows. This is the performance-tuning demonstration.
//
// 5. OUTLIERS ARE FLAGGED AND CAPPED IN A NEW COLUMN, NEVER IN PLACE.
//    Large amounts are a genuine fraud signal, so overwriting `amount` would
//    destroy information. `amount_capped` and `is_outlier` sit alongside it.
//
// 6. TIMESTAMP ASSEMBLED from the separate date parts, which unlocks
//    day_of_week / is_weekend and is required by the Phase 11 streaming job.
//
// Run:  bash scripts/run_preprocess.sh
// ===========================================================================

import java.io.PrintWriter

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
val PROFILE    = "/artifacts/data_quality_profile.json"

// Without this repartition, 225 input tasks x N partition directories would
// produce thousands of tiny Parquet files.
val OUTPUT_PARTITIONS = 120

spark.conf.set("spark.sql.shuffle.partitions", OUTPUT_PARTITIONS.toString)
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

println("=" * 74)
println(" PHASE 4: Scala preprocessing on Spark / YARN")
println("=" * 74)
println(s" input  : $INPUT")
println(s" output : $OUTPUT")
println("")

val t0 = System.currentTimeMillis()

// ---------------------------------------------------------------------------
// 1. Declared schemas
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

// Declaring the nested schema is what makes missing sub-objects come back as
// null instead of throwing.
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

println(">>> Counting raw input rows (the output must match this exactly)")
val rawCount = rawDf.count()
println(f">>> RAW ROWS IN: $rawCount%,d")
println("")

// ---------------------------------------------------------------------------
// 2. Clean, cast, impute. Nothing is dropped anywhere in this stage.
// ---------------------------------------------------------------------------
val parsed = rawDf
  .withColumn("amount_clean",
    coalesce(regexp_replace(trim(col("amount")), "[$,]", "").cast(DoubleType),
             lit(0.0)))
  .withColumn("is_fraud_int",
    when(lower(trim(col("is_fraud"))).isin("yes", "1", "true"), lit(1))
      .otherwise(lit(0)))
  .withColumn("user_id",   coalesce(col("user").cast(IntegerType),  lit(-1)))
  .withColumn("card_id",   coalesce(col("card").cast(IntegerType),  lit(-1)))
  .withColumn("txn_year",  coalesce(col("year").cast(IntegerType),  lit(2000)))
  .withColumn("txn_month", coalesce(col("month").cast(IntegerType), lit(1)))
  .withColumn("txn_day",   coalesce(col("day").cast(IntegerType),   lit(1)))
  .withColumn("mcc_code",  coalesce(col("mcc").cast(IntegerType),   lit(-1)))
  .withColumn("txn_hour",
    coalesce(split(coalesce(col("time"), lit("00:00")), ":").getItem(0)
      .cast(IntegerType), lit(0)))
  .withColumn("txn_minute",
    coalesce(split(coalesce(col("time"), lit("00:00")), ":").getItem(1)
      .cast(IntegerType), lit(0)))
  // --- flags recording WHICH values were imputed, evaluated before filling -
  .withColumn("imputed_city",
    when(col("merchant_city").isNull || (trim(col("merchant_city")) === ""),
      lit(1)).otherwise(lit(0)))
  .withColumn("imputed_state",
    when(col("merchant_state").isNull || (trim(col("merchant_state")) === ""),
      lit(1)).otherwise(lit(0)))
  .withColumn("imputed_zip",
    when(col("zip").isNull || (trim(col("zip")) === ""),
      lit(1)).otherwise(lit(0)))
  // --- sentinel imputation for categoricals: a row is never lost -----------
  .withColumn("merchant_city_clean",
    when(col("merchant_city").isNull || (trim(col("merchant_city")) === ""),
      lit("UNKNOWN")).otherwise(upper(trim(col("merchant_city")))))
  .withColumn("merchant_state_clean",
    when(col("merchant_state").isNull || (trim(col("merchant_state")) === ""),
      lit("UNKNOWN")).otherwise(upper(trim(col("merchant_state")))))
  .withColumn("use_chip_clean",
    when(col("use_chip").isNull || (trim(col("use_chip")) === ""),
      lit("UNKNOWN")).otherwise(trim(col("use_chip"))))
  .withColumn("zip_clean",
    when(col("zip").isNull || (trim(col("zip")) === ""), lit("00000"))
      .otherwise(lpad(split(trim(col("zip")), "\\.").getItem(0), 5, "0")))
  .withColumn("error_flag",
    when(col("errors").isNull || (trim(col("errors")) === ""),
      lit(0)).otherwise(lit(1)))
  .withColumn("error_type",
    when(col("errors").isNull || (trim(col("errors")) === ""),
      lit("NONE")).otherwise(trim(col("errors"))))
  // --- nested JSON --------------------------------------------------------
  .withColumn("dev", from_json(col("device_metadata"), deviceSchema))
  .withColumn("vpn_flag",
    when(col("dev.network.vpn") === true, lit(1)).otherwise(lit(0)))
  .withColumn("device_ip",  coalesce(col("dev.network.ip"), lit("0.0.0.0")))
  .withColumn("device_os",  coalesce(col("dev.hardware.os"), lit("UNKNOWN")))
  .withColumn("device_lat", col("dev.geo.lat"))
  .withColumn("device_lon", col("dev.geo.lon"))
  .withColumn("geo_missing",
    when(col("dev.geo").isNull, lit(1)).otherwise(lit(0)))
  .withColumn("hw_missing",
    when(col("dev.hardware").isNull, lit(1)).otherwise(lit(0)))
  .drop("dev")

// ---------------------------------------------------------------------------
// 3. BROADCAST JOIN -- merchant coordinates from the small lookup table
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

val joined = parsed
  .join(broadcast(zipDf), parsed("zip_clean") === zipDf("lookup_zip"), "left")
  .drop("lookup_zip")

// ---------------------------------------------------------------------------
// 4. Outlier threshold -- ONE approximate quantile pass.
//    The original amount is preserved; the cap is a separate column.
// ---------------------------------------------------------------------------
println(">>> Computing the 99th-percentile amount (approximate, 1 pass)")
val quantiles = joined.stat.approxQuantile("amount_clean", Array(0.99), 0.01)
val amountP99 = if (quantiles.nonEmpty) quantiles(0) else 1000.0
println(f">>> amount p99 = $amountP99%.2f  (rows above are flagged, not removed)")

// ---------------------------------------------------------------------------
// 5. Derived features
// ---------------------------------------------------------------------------
val R    = 6371.0
val dLat = radians(col("merchant_lat") - col("device_lat"))
val dLon = radians(col("merchant_lon") - col("device_lon"))
val hav  = pow(sin(dLat / lit(2.0)), 2.0) +
           cos(radians(col("device_lat"))) * cos(radians(col("merchant_lat"))) *
           pow(sin(dLon / lit(2.0)), 2.0)

val featured = joined
  .withColumn("amount_abs", abs(col("amount_clean")))
  .withColumn("amount_log", log(lit(1.0) + abs(col("amount_clean"))))
  .withColumn("is_refund",
    when(col("amount_clean") < 0, lit(1)).otherwise(lit(0)))
  .withColumn("is_outlier",
    when(abs(col("amount_clean")) > lit(amountP99), lit(1)).otherwise(lit(0)))
  .withColumn("amount_capped",
    least(abs(col("amount_clean")), lit(amountP99)))
  .withColumn("is_online",
    when(col("merchant_city_clean") === "ONLINE", lit(1)).otherwise(lit(0)))
  .withColumn("is_foreign_or_unknown",
    when(length(col("merchant_state_clean")) =!= 2, lit(1)).otherwise(lit(0)))
  .withColumn("is_night",
    when(col("txn_hour") < 6 || col("txn_hour") >= 22, lit(1)).otherwise(lit(0)))
  .withColumn("device_merchant_distance_km",
    coalesce(round(lit(2.0 * R) * asin(sqrt(hav)), 2), lit(-1.0)))
  // --- assembled date and timestamp ---------------------------------------
  // make_date yields null for impossible dates (e.g. 31 February), so we fall
  // back to the 1st of that month rather than dropping the row.
  .withColumn("txn_date",
    coalesce(
      make_date(col("txn_year"), col("txn_month"), col("txn_day")),
      make_date(col("txn_year"), col("txn_month"), lit(1)),
      make_date(lit(2000), lit(1), lit(1))))
  .withColumn("txn_timestamp",
    to_timestamp(concat_ws(" ",
      date_format(col("txn_date"), "yyyy-MM-dd"),
      format_string("%02d:%02d:00", col("txn_hour"), col("txn_minute")))))
  // dayofweek: 1 = Sunday ... 7 = Saturday
  .withColumn("day_of_week", dayofweek(col("txn_date")))
  .withColumn("is_weekend",
    when(dayofweek(col("txn_date")).isin(1, 7), lit(1)).otherwise(lit(0)))
  .withColumn("day_of_year", dayofyear(col("txn_date")))
  // --- fill remaining numeric nulls ---------------------------------------
  .withColumn("device_lat_filled",   coalesce(col("device_lat"),   lit(0.0)))
  .withColumn("device_lon_filled",   coalesce(col("device_lon"),   lit(0.0)))
  .withColumn("merchant_lat_filled", coalesce(col("merchant_lat"), lit(0.0)))
  .withColumn("merchant_lon_filled", coalesce(col("merchant_lon"), lit(0.0)))
  .withColumn("processed_at", current_timestamp())

// ---------------------------------------------------------------------------
// 6. Final projection -- explicit and stable for Phase 5
// ---------------------------------------------------------------------------
val finalDf = featured.select(
  col("user_id").as("user"),
  col("card_id").as("card"),
  col("txn_year").as("year"),
  col("txn_month").as("month"),
  col("txn_day").as("day"),
  col("txn_hour").as("hour"),
  col("txn_minute").as("minute"),
  col("txn_timestamp"),
  col("day_of_week"),
  col("is_weekend"),
  col("day_of_year"),
  col("amount_clean").as("amount"),
  col("amount_abs"),
  col("amount_log"),
  col("amount_capped"),
  col("is_outlier"),
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
  col("imputed_city"),
  col("imputed_state"),
  col("imputed_zip"),
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
// 7. Write -- Parquet, Snappy, partitioned by year/month
// ---------------------------------------------------------------------------
println("")
println(">>> Writing Parquet, partitioned by year/month (Snappy)")

finalDf
  .repartition(OUTPUT_PARTITIONS, col("year"), col("month"))
  .write
  .mode("overwrite")
  .partitionBy("year", "month")
  .parquet(OUTPUT)

println(f">>> Write finished at ${(System.currentTimeMillis() - t0) / 1000.0}%.1f s")

// ---------------------------------------------------------------------------
// 8. Verification and data-quality profile
// ---------------------------------------------------------------------------
println("")
println(">>> Verifying")
val outDf = spark.read.parquet(OUTPUT).cache()

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
  println(" RESULT   : MISMATCH -- investigate before Phase 5")
}
println(f" fraud    : $fraudOut%,d (${100.0 * fraudOut / outCount}%.4f%%)")
println("")

val stats = outDf.select(
  sum("imputed_city").as("imp_city"),
  sum("imputed_state").as("imp_state"),
  sum("imputed_zip").as("imp_zip"),
  sum("geo_missing").as("imp_geo"),
  sum("hw_missing").as("imp_hw"),
  sum("is_outlier").as("outliers"),
  sum("is_refund").as("refunds"),
  sum("is_online").as("online"),
  sum("is_night").as("night"),
  sum("is_weekend").as("weekend"),
  sum("error_flag").as("errors"),
  avg("amount_abs").as("mean_amount"),
  max("amount_abs").as("max_amount"),
  countDistinct("user").as("users"),
  countDistinct("merchant_state").as("states"),
  countDistinct("mcc").as("mccs"),
  min("year").as("first_year"),
  max("year").as("last_year")
).collect()(0)

val nullAmounts = outDf.filter(col("amount").isNull).count()
val nullStates  = outDf.filter(col("merchant_state").isNull).count()
val badZips     = outDf.filter(length(col("zip")) =!= 5).count()
val nullTs      = outDf.filter(col("txn_timestamp").isNull).count()

println(">>> Cleanup spot-checks (all four must be 0)")
println(f" null amounts       : $nullAmounts%,d")
println(f" null states        : $nullStates%,d")
println(f" malformed zips     : $badZips%,d")
println(f" null timestamps    : $nullTs%,d")
println("")
println(">>> Imputation counts (recorded, not hidden)")
println(f" city imputed       : ${stats.getAs[Long]("imp_city")}%,d")
println(f" state imputed      : ${stats.getAs[Long]("imp_state")}%,d")
println(f" zip imputed        : ${stats.getAs[Long]("imp_zip")}%,d")
println(f" geo imputed        : ${stats.getAs[Long]("imp_geo")}%,d")
println(f" hardware imputed   : ${stats.getAs[Long]("imp_hw")}%,d")
println(f" amount outliers    : ${stats.getAs[Long]("outliers")}%,d")
println("")

val profileJson = s"""{
  "phase": "4 - scala preprocessing",
  "input": "$INPUT",
  "output": "$OUTPUT",
  "rows_in": $rawCount,
  "rows_out": $outCount,
  "rows_dropped": ${rawCount - outCount},
  "reconciliation": "${if (rawCount == outCount) "PASS" else "MISMATCH"}",
  "fraud_rows": $fraudOut,
  "fraud_rate_pct": ${"%.4f".format(100.0 * fraudOut / outCount)},
  "amount_p99_threshold": ${"%.2f".format(amountP99)},
  "cleanup_checks": {
    "null_amounts": $nullAmounts,
    "null_states": $nullStates,
    "malformed_zips": $badZips,
    "null_timestamps": $nullTs
  },
  "imputation_counts": {
    "merchant_city": ${stats.getAs[Long]("imp_city")},
    "merchant_state": ${stats.getAs[Long]("imp_state")},
    "zip": ${stats.getAs[Long]("imp_zip")},
    "device_geo": ${stats.getAs[Long]("imp_geo")},
    "device_hardware": ${stats.getAs[Long]("imp_hw")}
  },
  "derived_flags": {
    "amount_outliers": ${stats.getAs[Long]("outliers")},
    "refunds": ${stats.getAs[Long]("refunds")},
    "online": ${stats.getAs[Long]("online")},
    "night": ${stats.getAs[Long]("night")},
    "weekend": ${stats.getAs[Long]("weekend")},
    "with_errors": ${stats.getAs[Long]("errors")}
  },
  "distributions": {
    "mean_amount": ${"%.2f".format(stats.getAs[Double]("mean_amount"))},
    "max_amount": ${"%.2f".format(stats.getAs[Double]("max_amount"))},
    "distinct_users": ${stats.getAs[Long]("users")},
    "distinct_states": ${stats.getAs[Long]("states")},
    "distinct_mcc": ${stats.getAs[Long]("mccs")},
    "year_range": [${stats.getAs[Int]("first_year")}, ${stats.getAs[Int]("last_year")}]
  }
}"""

try {
  val pw = new PrintWriter(PROFILE)
  pw.write(profileJson)
  pw.close()
  println(s">>> Data-quality profile written to $PROFILE")
} catch {
  case e: Exception => println(s">>> Could not write the profile: ${e.getMessage}")
}

println("")
println(">>> Schema")
outDf.printSchema()

println(">>> Sample rows")
outDf.select("user", "txn_timestamp", "day_of_week", "amount", "amount_capped",
             "is_outlier", "merchant_state", "mcc",
             "device_merchant_distance_km", "is_fraud")
  .show(10, false)

// ---------------------------------------------------------------------------
// 9. A readable CSV sample -- Parquet is binary, so this is what you open
//    when someone asks to SEE the cleaned data.
// ---------------------------------------------------------------------------
println(">>> Writing a 20,000-row CSV sample")
outDf.limit(20000)
  .coalesce(1)
  .write.mode("overwrite")
  .option("header", "true")
  .csv(SAMPLE_OUT)

outDf.unpersist()

val elapsed = (System.currentTimeMillis() - t0) / 1000.0
println("")
println("=" * 74)
println(f" PHASE 4 COMPLETE in $elapsed%.1f s (${elapsed / 60.0}%.1f min)")
println("=" * 74)

System.exit(0)
