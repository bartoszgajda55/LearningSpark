import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions.from_json

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

    val rsvpJsonDF = df.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("rsvp_id", StringType)
      .add("api_version", StringType)
      .add("event", new StructType()
        .add("api_version", StringType)
        .add("event_id", StringType)
        .add("event_name", StringType)
        .add("event_url", StringType)
        .add("time", StringType))
      .add("group", new StructType()
        .add("api_version", StringType)
        .add("group_city", StringType)
        .add("group_country", StringType)
        .add("group_id", StringType)
        .add("group_lat", StringType)
        .add("group_lon", StringType)
        .add("group_name", StringType)
        .add("group_state", StringType)
      .add("group_topics", new ArrayType(new StructType()
          .add("api_version", StringType)
          .add("topic_name", StringType)
          .add("urlkey", StringType), true))
        .add("group_urlname", StringType))
      .add("guests", StringType)
      .add("member", new StructType()
        .add("api_version", StringType)
        .add("member_id", StringType)
        .add("member_name", StringType)
        .add("other_services", StringType)
        .add("photo", StringType))
      .add("mtime", StringType)
      .add("response", StringType)
      .add("venue", new StructType()
        .add("api_version", StringType)
        .add("lat", StringType)
        .add("lon", StringType)
        .add("venue_id", StringType)
        .add("venue_name", StringType))

    val rsvpNestedDf = rsvpJsonDF.select(from_json($"value", struct).as("rsvp"))
    val rsvpFlattenedDf = rsvpNestedDf.selectExpr("rsvp.rsvp_id", "rsvp.event.event_name", "rsvp.group.group_name")

    val consoleOutput = rsvpFlattenedDf.writeStream
      .outputMode("append")
      .format("console")
      .start()
    consoleOutput.awaitTermination()
  }
}
