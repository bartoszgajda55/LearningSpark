import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
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

    val peopleDF = spark.sparkContext
      .textFile("resources/people.txt")
      .map(_.split(", "))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    // Access field by index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    // Access field by name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    spark.udf.register("myAverage", UserDefinedAggregate)

    val df2 = spark.read.json("resources/employees.json")
    df2.createOrReplaceTempView("employees")
    df2.show()

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
  }
}
