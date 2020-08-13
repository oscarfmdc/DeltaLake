import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, sum}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingPerfTest {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.err.println("Needs 1 argument")
      System.exit(1)
    }

    val delta_location = args(0) + "/delta-table"
    val results_location = args(0) + "/results"
    val checkpoint_location = args(0) + "/checkpoints2"

    val conf = new SparkConf().setAppName("StreamingPerfTest")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    spark.readStream
      .format("delta")
      .load(delta_location)
      .select("value")
      .withColumn("wordCount", size(split(col("value"), " ")))
      .agg(sum("wordCount").cast("long"))
      .writeStream
      .format("delta")
      .option("path", results_location)
      .option("checkpointLocation", checkpoint_location)
      .start()
  }
}
