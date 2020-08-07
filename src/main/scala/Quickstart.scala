import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Tests delta lake in Azure Databricks environment
 */
object Quickstart {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("Delta Lake Tests")
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

        val eventsPath = args(0)
        val format = args(1)
        val iterations = args(2).toInt

        if (format != "parquet" && format != "delta") {
            System.exit(1)
        }
        if (iterations < 1) {
            System.exit(1)
        }

        // Read example dataset
        val events: DataFrame = spark.read
          .option("inferSchema", "true")
          .json("/databricks-datasets/structured-streaming/events/")
          .withColumn("date", expr("time"))
          .drop("time")
          .withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd"))

        // Write dataset with the specified format in the specified path
        events.write.format(format).mode("overwrite").partitionBy("date").save(eventsPath + "events/")

        // Append historical events to dataset
        for( w <- 0 to iterations){
            val historical_events = spark.read
              .option("inferSchema", "true")
              .json("/databricks-datasets/structured-streaming/events/")
              .withColumn("date", expr("time-1" + w + "28000"))
              .drop("time")
              .withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd"))

            historical_events.write.format(format).mode("append").partitionBy("date").save(eventsPath + "events/")
        }

        // Read dataset Again
        val events_delta = spark.read.format(format).load(eventsPath + "events/")

        // Create spark table
        spark.sql("DROP TABLE IF EXISTS events")
        spark.sql("CREATE TABLE events USING " + format + " LOCATION '" + eventsPath + "events/'")
        events_delta.count()

        if (format == "delta") {
            spark.sql("OPTIMIZE events ZORDER BY (action)")
        }

        // transform the full dataset and persist it
        events_delta
          .groupBy("action","date")
          .agg(count("action").alias("action_count"))
          .orderBy("date", "action")

        events_delta.write.format(format)
          .mode("overwrite")
          .partitionBy("date")
          .save(eventsPath + "grouped/")
    }
}
