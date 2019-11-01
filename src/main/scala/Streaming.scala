import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Streaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Streaming From Kafka")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rsvp")
      .load()

    val rsvpDF = df.selectExpr("CAST(value AS STRING)")

    val consoleOutput = rsvpDF.writeStream
      .trigger(Trigger.Continuous(1000))
      .outputMode("append")
      .format("console")
      .start()
    consoleOutput.awaitTermination()
  }
}
