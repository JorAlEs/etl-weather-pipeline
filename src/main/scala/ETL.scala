import org.apache.spark.sql.SparkSession

object ETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Weather + Health ETL")
      .master("local[*]")
      .getOrCreate()

    println("Spark session started")

    spark.stop()
  }
}
