import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, from_unixtime}
import org.apache.spark.sql.functions._

/**
 * Tests delta lake in Azure Databricks environment
 */
object TestDataset {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Delta Lake Tests Dataset")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val resultsPath = args(0)
    val format = args(1)
    val iterations = args(2).toInt
    val datasetPath = args(3)

    if (format != "parquet" && format != "delta") {
      System.exit(1)
    }
    if (iterations < 1) {
      System.exit(1)
    }

    // Read dataset
    val events: DataFrame = spark.read
      .option("inferSchema", "true")
      .csv(datasetPath)
      .withColumn("time", unix_timestamp())
      .withColumn("date", from_unixtime(col("time"), "yyyy-MM-dd"))

    // Write dataset with the specified format in the specified path
    events.write.format(format).mode("overwrite").partitionBy("date").save(resultsPath + "events/")

    // Append historical events to dataset
    for( w <- 0 to iterations){
      val historical_events = spark.read
        .option("inferSchema", "true")
        .csv(datasetPath)
        .withColumn("date", unix_timestamp())
        .withColumn("time", unix_timestamp() - 172800 * w)
        .withColumn("date", from_unixtime(col("time"), "yyyy-MM-dd"))

      historical_events.write.format(format).mode("append").partitionBy("date").save(resultsPath + "events/")
    }

    // Read dataset Again
    val events_delta = spark.read.format(format).load(resultsPath + "events/")

    // Create spark table
    spark.sql("DROP TABLE IF EXISTS events")
    spark.sql("CREATE TABLE events USING " + format + " LOCATION '" + resultsPath + "events/'")
    events_delta.count()

    if (format == "delta") {
      spark.sql("OPTIMIZE events ZORDER BY (_c1)")
    }

    // transform the full dataset and persist it
    events_delta
      .groupBy("_c1", "date")
      .agg(count("_c1").alias("count"))
      .orderBy("date", "_c1")

    events_delta.write.format(format)
      .mode("overwrite")
      .partitionBy("date")
      .save(resultsPath + "grouped/")
  }
}
