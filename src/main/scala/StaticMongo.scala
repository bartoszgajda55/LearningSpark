import org.apache.spark.sql.SparkSession
import com.mongodb.spark._

object StaticMongo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Streaming From Kafka")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/spark.FirstCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/spark.FirstCollection")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    import org.bson.Document

    val docs = """
        {"name": "Bilbo Baggins", "age": 50}
        {"name": "Gandalf", "age": 1000}
        {"name": "Thorin", "age": 195}
        {"name": "Balin", "age": 178}
        {"name": "Kíli", "age": 77}
        {"name": "Dwalin", "age": 169}
        {"name": "Óin", "age": 167}
        {"name": "Glóin", "age": 158}
        {"name": "Fíli", "age": 82}
        {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
    spark.sparkContext.parallelize(docs.map(Document.parse)).saveToMongoDB()
  }
}
