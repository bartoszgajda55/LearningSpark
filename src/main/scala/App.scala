import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object App {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("Spark First Example")
      .master("local[*]")
      .getOrCreate()
    val df = spark.read.json("resources/people.json")
    df.show()

    import spark.implicits._
    df.printSchema()
    df.select("name")
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    // Creating DataSets
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect()

    case class Person(name: String, age: Long)

    val path = "resources/people.json"
//    val peopleDS = spark.read.json(path).as[Person]
//    peopleDS.show()
  }
}
