import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ETL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Weather + Health ETL")
      .master("local[*]")
      .getOrCreate()

    println("Spark session started")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/raw/santiago_real_weather.csv")

    val dfWithTimestamp = df.withColumn("timestamp", to_timestamp(col("fecha_hora"), "dd/MM/yyyy HH:mm"))
    dfWithTimestamp.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/clean/santiago_weather_clean.csv")
  }
}

// This code reads a CSV file containing weather data, converts the date and time column to a timestamp format,
// and writes the cleaned data to a new CSV file. The Spark session is configured to run locally with all available cores.
// The input file is expected to be in the "data/raw" directory and the output will be saved in the "data/clean" directory.
// Make sure to adjust the file paths as necessary for your environment.
// The Spark session is initialized with the application name "Weather + Health ETL".
// The code uses the Spark SQL functions to handle date and time conversions efficiently.
// Ensure that the Spark dependencies are included in your build tool (e.g., sbt, Maven) to run this code successfully.
// This ETL process is a basic example and can be extended to include more complex transformations or additional data sources as needed.    