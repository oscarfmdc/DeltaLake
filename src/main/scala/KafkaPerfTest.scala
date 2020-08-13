import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}

object KafkaPerfTest {
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Needs 4 arguments")
      System.exit(1)
    }

    val brokers = args(0)
    val groupId = args(1)
    val topic = args(2)
    val delta_location = args(3) + "/delta-table"
    val checkpoint_location = args(3) + "/checkpoints"

    val conf = new SparkConf().setAppName("KafkaPerfTest")
    val sc = SparkContext.getOrCreate(conf)
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

  }

}
