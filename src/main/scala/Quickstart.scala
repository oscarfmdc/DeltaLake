import org.apache.spark.sql.functions._
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Quickstart {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("Delta Lake Tests")
        val sc = SparkContext.getOrCreate(conf)
        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

        val eventsPath = args(0)
        // delta/parquet
        val format = args(1)

        // Read example dataset
        val events: DataFrame = spark.read
          .option("inferSchema", "true")
          .json("/databricks-datasets/structured-streaming/events/")
          .withColumn("date", expr("time"))
          .drop("time")
          .withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd"))

        // Write dataset in with the specified format in the specified path
        events.write.format(format).mode("overwrite").partitionBy("date").save(eventsPath + "events/")

        for( w <- 0 to 100){
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
            spark.sql("OPTIMIZE events")
        }

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
