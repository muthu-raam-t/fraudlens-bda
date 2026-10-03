// ===========================================================================
// PHASE 12b -- Kafka-sourced Structured Streaming fraud scorer.
//
// WHAT CHANGES FROM PHASE 11
//   Phase 11 watched an HDFS directory: a script wrote a CSV, Spark noticed
//   the file. That is file-polling. Here Spark SUBSCRIBES to a Kafka topic and
//   consumes individual transaction events as they are published -- genuinely
//   event-driven, which is how a payment switch delivers transactions.
//
//   Everything downstream is unchanged: the same GBT PipelineModel trained on
//   all 132,500,000 rows, the same broadcast stream-static join, the same
//   verdict output. Only the source differs.
//
// WHY SCALA
//   spark-master ships Python 3.7 (Alpine) and nodemanager ships 3.5
//   (Debian 9). PySpark refuses to run across minor versions. Scala launches
//   no Python workers, so the problem cannot arise.
//
// KAFKA CONNECTOR
//   spark-sql-kafka-0-10 is NOT bundled with Spark. run_kafka_stream.sh passes
//   it via --packages, which downloads it from Maven Central on first run and
//   caches it in the container afterwards.
//
// Run:  bash scripts/run_kafka_stream.sh
//       bash scripts/run_producer.sh      (second terminal)
// ===========================================================================

import java.io.PrintWriter

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val HDFS        = "hdfs://namenode:9000"
val BATCH_PATH  = HDFS + "/fraudlens/dataset/preprocessed"
val MODEL_PATH  = HDFS + "/fraudlens/models/gbt"
val OUT_DIR     = HDFS + "/fraudlens/streaming/kafka_verdicts"
val CKPT_DIR    = HDFS + "/fraudlens/streaming/kafka_checkpoint"
val LOCAL_OUT   = "/artifacts/stream_verdicts"
val KAFKA_BOOT  = "kafka:9092"
val IN_TOPIC    = "transactions"
val OUT_TOPIC   = "verdicts"
val TIMEOUT_MS  = 20L * 60L * 1000L

spark.conf.set("spark.sql.shuffle.partitions", "8")

println("=" * 70)
println(" PHASE 12: Kafka -> Spark Structured Streaming -> verdicts")
println("=" * 70)

// ---------------------------------------------------------------------------
// Schema of the JSON messages the producer publishes. Declared explicitly:
// Kafka delivers opaque bytes, so Spark has no schema to infer from.
// ---------------------------------------------------------------------------
val eventSchema = StructType(Array(
  StructField("user",                        IntegerType, true),
  StructField("card",                        IntegerType, true),
  StructField("day",                         IntegerType, true),
  StructField("hour",                        IntegerType, true),
  StructField("minute",                      IntegerType, true),
  StructField("day_of_week",                 IntegerType, true),
  StructField("is_weekend",                  IntegerType, true),
  StructField("day_of_year",                 IntegerType, true),
  StructField("amount",                      DoubleType,  true),
  StructField("amount_abs",                  DoubleType,  true),
  StructField("amount_log",                  DoubleType,  true),
  StructField("amount_capped",               DoubleType,  true),
  StructField("is_outlier",                  IntegerType, true),
  StructField("is_refund",                   IntegerType, true),
  StructField("use_chip",                    StringType,  true),
  StructField("merchant_name",               StringType,  true),
  StructField("merchant_city",               StringType,  true),
  StructField("merchant_state",              StringType,  true),
  StructField("zip",                         StringType,  true),
  StructField("mcc",                         IntegerType, true),
  StructField("error_flag",                  IntegerType, true),
  StructField("error_type",                  StringType,  true),
  StructField("vpn_flag",                    IntegerType, true),
  StructField("device_ip",                   StringType,  true),
  StructField("device_os",                   StringType,  true),
  StructField("device_lat",                  DoubleType,  true),
  StructField("device_lon",                  DoubleType,  true),
  StructField("geo_missing",                 IntegerType, true),
  StructField("hw_missing",                  IntegerType, true),
  StructField("imputed_city",                IntegerType, true),
  StructField("imputed_state",               IntegerType, true),
  StructField("imputed_zip",                 IntegerType, true),
  StructField("merchant_lat",                DoubleType,  true),
  StructField("merchant_lon",                DoubleType,  true),
  StructField("device_merchant_distance_km", DoubleType,  true),
  StructField("is_online",                   IntegerType, true),
  StructField("is_foreign_or_unknown",       IntegerType, true),
  StructField("is_night",                    IntegerType, true),
  StructField("is_fraud",                    IntegerType, true),
  StructField("event_id",                    IntegerType, true),
  StructField("emitted_at",                  StringType,  true)
))

// ---------------------------------------------------------------------------
// Batch-derived features, computed once and broadcast into the stream.
// A single Kafka message cannot know a user's historical spending average,
// so these come from the batch dataset as a stream-static join.
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

println(f">>> user stats: ${userStats.count()}%,d rows | mcc stats: ${mccFreq.count()}%,d rows")

println(">>> Loading the GBT PipelineModel trained on all 132,500,000 rows")
val model = PipelineModel.load(MODEL_PATH)
println(">>> Model loaded: " + model.stages.length + " pipeline stages")

// ---------------------------------------------------------------------------
// Subscribe to the Kafka topic
// ---------------------------------------------------------------------------
println(">>> Subscribing to Kafka topic '" + IN_TOPIC + "' at " + KAFKA_BOOT)

val kafkaRaw = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", KAFKA_BOOT)
  .option("subscribe", IN_TOPIC)
  .option("startingOffsets", "latest")
  .option("maxOffsetsPerTrigger", 200)
  .option("failOnDataLoss", "false")
  .load()

// Kafka hands over bytes. Cast the value to a string, then parse the JSON.
val events = kafkaRaw
  .select(from_json(col("value").cast("string"), eventSchema).as("e"),
          col("timestamp").as("kafka_timestamp"),
          col("partition").as("kafka_partition"),
          col("offset").as("kafka_offset"))
  .select(col("e.*"), col("kafka_timestamp"), col("kafka_partition"),
          col("kafka_offset"))
  .filter(col("user").isNotNull)

val enriched = events
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
  col("event_id"),
  col("user"),
  col("amount"),
  col("merchant_state"),
  col("mcc"),
  col("hour"),
  col("vpn_flag"),
  round(vector_to_array(col("probability"))(1), 6).as("fraud_probability"),
  col("prediction").cast(IntegerType).as("predicted_fraud"),
  col("is_fraud").as("actual_fraud"),
  col("kafka_partition"),
  col("kafka_offset"),
  col("emitted_at"),
  current_timestamp().as("scored_at")
)

// ---------------------------------------------------------------------------
// Sink: HDFS Parquet for the record, a Kafka topic for downstream consumers,
// and a local JSON slice the Streamlit dashboard tails.
// ---------------------------------------------------------------------------
new java.io.File(LOCAL_OUT).mkdirs()

var totalBatches = 0L
var totalRows    = 0L
var totalFlagged = 0L

val query = verdicts.writeStream
  .foreachBatch { (df: DataFrame, batchId: Long) =>
    val n = df.count()
    if (n > 0) {
      df.persist()
      val flagged = df.filter(col("predicted_fraud") === 1).count()
      totalBatches += 1
      totalRows    += n
      totalFlagged += flagged

      // 1. durable record
      df.write.mode("append").parquet(OUT_DIR)

      // 2. publish verdicts back to Kafka so other services could consume them
      df.select(col("user").cast("string").as("key"),
                to_json(struct(df.columns.map(col): _*)).as("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOT)
        .option("topic", OUT_TOPIC)
        .save()

      // 3. local JSON for the dashboard
      val sample = df.orderBy(desc("fraud_probability")).limit(40).collect()
      val pw = new PrintWriter(f"$LOCAL_OUT/kafka_batch_$batchId%05d.json")
      try {
        sample.foreach { r =>
          pw.println(
            "{\"event_id\":"          + r.getAs[Int]("event_id") +
            ",\"user\":"              + r.getAs[Int]("user") +
            ",\"amount\":"            + r.getAs[Double]("amount") +
            ",\"merchant_state\":\""  + r.getAs[String]("merchant_state") + "\"" +
            ",\"mcc\":"               + r.getAs[Int]("mcc") +
            ",\"hour\":"              + r.getAs[Int]("hour") +
            ",\"vpn_flag\":"          + r.getAs[Int]("vpn_flag") +
            ",\"fraud_probability\":" + r.getAs[Double]("fraud_probability") +
            ",\"predicted_fraud\":"   + r.getAs[Int]("predicted_fraud") +
            ",\"actual_fraud\":"      + r.getAs[Int]("actual_fraud") +
            ",\"kafka_partition\":"   + r.getAs[Int]("kafka_partition") +
            ",\"kafka_offset\":"      + r.getAs[Long]("kafka_offset") +
            ",\"batch_id\":"          + batchId +
            ",\"scored_at\":\""       + r.getAs[java.sql.Timestamp]("scored_at") + "\"}")
        }
      } finally {
        pw.close()
      }

      println(f"[kafka-stream] batch $batchId%-4d | $n%5d events | $flagged%4d flagged " +
              f"| totals: $totalRows%,d scored, $totalFlagged%,d flagged")
      df.unpersist()
    }
  }
  .outputMode("append")
  .option("checkpointLocation", CKPT_DIR)
  .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("4 seconds"))
  .start()

println("")
println("=" * 70)
println(" CONSUMING FROM KAFKA")
println(" Publish events:  bash scripts/run_producer.sh     (second terminal)")
println(" Live dashboard:  bash scripts/run_streamlit.sh    (third terminal)")
println(" Spark UI:        http://localhost:4040 -> Structured Streaming tab")
println(" Stops automatically after 20 minutes, or press Ctrl-C.")
println("=" * 70)
println("")

query.awaitTermination(TIMEOUT_MS)
query.stop()

println("")
println("=" * 70)
println(f" micro-batches processed : $totalBatches%,d")
println(f" events scored           : $totalRows%,d")
println(f" flagged as fraud        : $totalFlagged%,d")
println(" verdicts in HDFS        : " + OUT_DIR)
println(" verdicts topic          : " + OUT_TOPIC)
println("=" * 70)

System.exit(0)
