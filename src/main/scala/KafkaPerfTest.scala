import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, sum}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

object KafkaPerfTest {
  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <groupId> <topics>\n" +
        "  <brokers> is a list of one or more Kafka brokers\n" +
        "  <groupId> is a consumer group name to consume from topics\n" +
        "  <topics> is a list of one or more kafka topics to consume from\n\n")
      System.exit(1)
    }

    val brokers = args(0)
    val groupId = args(1)
    val topic = args(2)
    val seconds = args(3)
    val delta_location = args(4) + "/delta-table"
    val checkpoint_location = args(4) + "/checkpoints"
    val results_location = args(4) + "/results"

    val conf = new SparkConf().setAppName("KafkaPerfTest")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(seconds.toInt))
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("topic", topic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .filter(col("value").isNotNull) // Remove empty values
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpoint_location)
      .start(delta_location) // as a path

    // TODO: Separate into another main + Wordcount table

//    spark.readStream
//      .format("delta")
//      .load(delta_location)
//      .select("value")
//      .withColumn("wordCount", size(split(col("Description"), " ")))
//      .agg(sum("wordCount").cast("long"))
//      .writeStream
//      .format("delta")
//      .option("path", results_location)
//      .start()

    ssc.start()
    ssc.awaitTermination()
  }

}
