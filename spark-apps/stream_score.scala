// ===========================================================================
// PHASE 11 -- Spark Structured Streaming fraud scorer.   (VELOCITY)
//
// WHY SCALA AND NOT PYSPARK
//   The cluster images disagree on Python: spark-master is Alpine 3.10 with
//   Python 3.7 only, nodemanager is Debian 9 with Python 3.5 only. PySpark
//   refuses to run when driver and executor minor versions differ, and
//   neither image can install the other's version. Scala launches no Python
//   workers at all, so the problem cannot occur. Preprocessing (Phase 4) is
//   already Scala, so this keeps the distributed code in one language.
//
// WHAT IT DOES
//   Watches an HDFS directory. When a file lands, Structured Streaming picks
//   it up as a micro-batch, applies the SAME GBT PipelineModel that Phase 5
//   trained on all 132,500,000 rows, and writes checkpointed verdicts back
//   to HDFS.
//
// HONEST FRAMING
//   Structured Streaming is MICRO-BATCH, not per-event. That is how Spark
//   works, and it is what the report says.
//
// TWO THINGS THAT CATCH PEOPLE OUT, HANDLED HERE
//   1. A streaming file source CANNOT infer schema -- the StructType below is
//      declared explicitly and must match the preprocessed Parquet.
//   2. The saved PipelineModel expects engineered features (per-user amount
//      statistics, MCC frequency) that come from batch aggregates. They are
//      computed ONCE at startup and broadcast into the stream as a
//      stream-static join.
//
// Run:  bash scripts/run_streaming.sh
//       bash scripts/feed_stream.sh     (second terminal)
// ===========================================================================

import java.io.PrintWriter

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val HDFS       = "hdfs://namenode:9000"
val BATCH_PATH = HDFS + "/fraudlens/dataset/preprocessed"
val MODEL_PATH = HDFS + "/fraudlens/models/gbt"
val IN_DIR     = HDFS + "/fraudlens/streaming/input"
val OUT_DIR    = HDFS + "/fraudlens/streaming/verdicts"
val CKPT_DIR   = HDFS + "/fraudlens/streaming/checkpoint"
val LOCAL_OUT  = "/artifacts/stream_verdicts"
val TIMEOUT_MS = 30L * 60L * 1000L

spark.conf.set("spark.sql.shuffle.partitions", "8")

println("=" * 66)
println(" PHASE 11: Structured Streaming fraud scorer  (Scala)")
println("=" * 66)

// ---------------------------------------------------------------------------
// Declared schema -- streaming file sources cannot infer one.
// ---------------------------------------------------------------------------
val streamSchema = StructType(Array(
  StructField("user",                        IntegerType,   true),
  StructField("card",                        IntegerType,   true),
  StructField("day",                         IntegerType,   true),
  StructField("hour",                        IntegerType,   true),
  StructField("minute",                      IntegerType,   true),
  StructField("txn_timestamp",               TimestampType, true),
  StructField("day_of_week",                 IntegerType,   true),
  StructField("is_weekend",                  IntegerType,   true),
  StructField("day_of_year",                 IntegerType,   true),
  StructField("amount",                      DoubleType,    true),
  StructField("amount_abs",                  DoubleType,    true),
  StructField("amount_log",                  DoubleType,    true),
  StructField("amount_capped",               DoubleType,    true),
  StructField("is_outlier",                  IntegerType,   true),
  StructField("is_refund",                   IntegerType,   true),
  StructField("use_chip",                    StringType,    true),
  StructField("merchant_name",               StringType,    true),
  StructField("merchant_city",               StringType,    true),
  StructField("merchant_state",              StringType,    true),
  StructField("zip",                         StringType,    true),
  StructField("mcc",                         IntegerType,   true),
  StructField("error_flag",                  IntegerType,   true),
  StructField("error_type",                  StringType,    true),
  StructField("vpn_flag",                    IntegerType,   true),
  StructField("device_ip",                   StringType,    true),
  StructField("device_os",                   StringType,    true),
  StructField("device_lat",                  DoubleType,    true),
  StructField("device_lon",                  DoubleType,    true),
  StructField("geo_missing",                 IntegerType,   true),
  StructField("hw_missing",                  IntegerType,   true),
  StructField("imputed_city",                IntegerType,   true),
  StructField("imputed_state",               IntegerType,   true),
  StructField("imputed_zip",                 IntegerType,   true),
  StructField("merchant_lat",                DoubleType,    true),
  StructField("merchant_lon",                DoubleType,    true),
  StructField("device_merchant_distance_km", DoubleType,    true),
  StructField("is_online",                   IntegerType,   true),
  StructField("is_foreign_or_unknown",       IntegerType,   true),
  StructField("is_night",                    IntegerType,   true),
  StructField("is_fraud",                    IntegerType,   true),
  StructField("processed_at",                TimestampType, true),
  StructField("year",                        IntegerType,   true),
  StructField("month",                       IntegerType,   true)
))

// ---------------------------------------------------------------------------
// Batch-derived features, computed once and broadcast into the stream.
// ---------------------------------------------------------------------------
println(">>> Computing user and MCC statistics from the batch dataset")
val batchDf = spark.read.parquet(BATCH_PATH)

val userStats = batchDf.groupBy("user")
  .agg(avg("amount_abs").as("user_amount_mean"),
       stddev("amount_abs").as("user_amount_std"),
       count("*").as("user_txn_count"))
  .na.fill(Map("user_amount_std" -> 0.0))
  .cache()

val mccFreq = batchDf.groupBy("mcc")
  .agg(count("*").as("mcc_frequency"))
  .cache()

println(f">>> user stats: ${userStats.count()}%,d rows | " +
        f"mcc stats: ${mccFreq.count()}%,d rows")

println(">>> Loading the GBT PipelineModel trained in Phase 5")
val model = PipelineModel.load(MODEL_PATH)
println(">>> Model loaded: " + model.stages.length + " pipeline stages")

// ---------------------------------------------------------------------------
// The stream
// ---------------------------------------------------------------------------
println(">>> Watching " + IN_DIR)
val rawStream = spark.readStream
  .schema(streamSchema)
  .option("header", "true")
  .option("maxFilesPerTrigger", 1)
  .csv(IN_DIR)

val enriched = rawStream
  .join(broadcast(userStats), Seq("user"), "left")
  .join(broadcast(mccFreq), Seq("mcc"), "left")
  .withColumn("amount_vs_user_mean",
    col("amount_abs") / (coalesce(col("user_amount_mean"), lit(0.0)) + lit(1.0)))
  .withColumn("amount_zscore",
    (col("amount_abs") - coalesce(col("user_amount_mean"), lit(0.0))) /
    (coalesce(col("user_amount_std"), lit(0.0)) + lit(1.0)))
  .na.fill(0.0, Array("user_amount_mean", "user_amount_std", "user_txn_count",
                      "amount_vs_user_mean", "amount_zscore", "mcc_frequency"))

val scored = model.transform(enriched)

val verdicts = scored.select(
  col("user"),
  col("amount"),
  col("merchant_state"),
  col("mcc"),
  col("hour"),
  round(vector_to_array(col("probability"))(1), 6).as("fraud_probability"),
  col("prediction").cast(IntegerType).as("predicted_fraud"),
  col("is_fraud").as("actual_fraud"),
  current_timestamp().as("scored_at")
)

// ---------------------------------------------------------------------------
// Sink: persist every verdict to HDFS, mirror a slice locally for the API.
// ---------------------------------------------------------------------------
new java.io.File(LOCAL_OUT).mkdirs()

var totalBatches = 0L
var totalRows    = 0L
var totalFlagged = 0L

val query = verdicts.writeStream
  .foreachBatch { (df: DataFrame, batchId: Long) =>
    val n = df.count()
    if (n > 0) {
      val flagged = df.filter(col("predicted_fraud") === 1).count()
      totalBatches += 1
      totalRows    += n
      totalFlagged += flagged

      df.write.mode("append").parquet(OUT_DIR)

      val sample = df.orderBy(desc("fraud_probability")).limit(25).collect()
      val pw = new PrintWriter(f"$LOCAL_OUT/batch_$batchId%05d.json")
      try {
        sample.foreach { r =>
          pw.println(
            "{\"user\":" + r.getAs[Int]("user") +
            ",\"amount\":" + r.getAs[Double]("amount") +
            ",\"merchant_state\":\"" + r.getAs[String]("merchant_state") + "\"" +
            ",\"mcc\":" + r.getAs[Int]("mcc") +
            ",\"hour\":" + r.getAs[Int]("hour") +
            ",\"fraud_probability\":" + r.getAs[Double]("fraud_probability") +
            ",\"predicted_fraud\":" + r.getAs[Int]("predicted_fraud") +
            ",\"actual_fraud\":" + r.getAs[Int]("actual_fraud") +
            ",\"scored_at\":\"" + r.getAs[java.sql.Timestamp]("scored_at") + "\"}")
        }
      } finally {
        pw.close()
      }

      println(f"[stream] batch $batchId%-4d | $n%6d rows | $flagged%5d flagged " +
              f"| totals: $totalRows%,d rows, $totalFlagged%,d flagged")
    }
  }
  .outputMode("append")
  .option("checkpointLocation", CKPT_DIR)
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
  .start()

println("")
println("=" * 66)
println(" STREAM RUNNING")
println(" Feed it:  bash scripts/feed_stream.sh    (second terminal)")
println(" Watch it: http://localhost:4040 -> Structured Streaming tab")
println(" Stops automatically after 30 minutes, or press Ctrl-C.")
println("=" * 66)
println("")

query.awaitTermination(TIMEOUT_MS)
query.stop()

println("")
println("=" * 66)
println(f" micro-batches processed : $totalBatches%,d")
println(f" rows scored             : $totalRows%,d")
println(f" flagged as fraud        : $totalFlagged%,d")
println(" verdicts in HDFS        : " + OUT_DIR)
println("=" * 66)

System.exit(0)
