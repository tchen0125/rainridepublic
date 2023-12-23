import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val ingestionPath = "/user/bj2351_nyu_edu/final/data/"
val inputFile = ingestionPath + "hourly-weather-nyc-2022.csv"

val df: DataFrame = spark.read.option("header", true).csv(inputFile)

// We will only use data from the Central Park.
// The STATION column is not needed.

val weatherDF: DataFrame = df.filter('STATION === "72505394728").drop("STATION")

// 1. select useful columns for each report type
val fm15 = weatherDF
  .filter('REPORT_TYPE === "FM-15")
  .drop("REPORT_TYPE", "DailyWeather", "Sunrise", "Sunset")

val fm16 = weatherDF
  .filter('REPORT_TYPE === "FM-16")
  .drop("REPORT_TYPE", "DailyWeather", "Sunrise", "Sunset")

val sod  = weatherDF
  .filter('REPORT_TYPE === "SOD  ")
  .drop("REPORT_TYPE")
  .select("DATE", "SOURCE", "DailyWeather", "Sunrise", "Sunset")

val weatherConditions =
  weatherDF
  .withColumn("weather", explode(split($"DailyWeather", " ")))
  .select("weather")
  .distinct()
  .filter($"weather" =!= "")
  .as[String]
  .collect()
  .toList

println("Weather Conditions" + weatherConditions)

// 2. Processing SOD (adding boolean columns)
val sod1 = sod.withColumn("Rain", col("DailyWeather").contains("RA")).na.fill(Map("Rain" -> false))
val sod2 = sod1.withColumn("Snow", col("DailyWeather").contains("SN")).na.fill(Map("Snow" -> false))
val sod3 = sod2.withColumn("Mist", col("DailyWeather").contains("BR")).na.fill(Map("Mist" -> false))
val sod4 = sod3.withColumn("Haze", col("DailyWeather").contains("HZ")).na.fill(Map("Haze" -> false))
val sod5 = sod4.withColumn("Fog", col("DailyWeather").contains("FG")).na.fill(Map("Fog" -> false))
val sodWithB = sod5.withColumn("date_only", to_date(col("DATE"))).orderBy("date_only").drop("SOURCE")
val sodResult = sodWithB.select("date_only", "DATE", "Sunrise", "Sunset", "Rain", "Snow", "Mist", "Haze", "Fog")
println(sodResult.count())
sodResult.show(5)

// 3. Processing FM15 (Hourly Weather Data)
// Need min/max temperature per day

val profTemp = fm15
  .withColumn("date_only", to_date(col("DATE")))
  .groupBy("date_only")
  .agg(
    min("HourlyDryBulbTemperature").alias("MinTemp"),
    max("HourlyDryBulbTemperature").alias("MaxTemp"),
    avg("HourlyDryBulbTemperature").alias("AvgTemp")
  )
  .orderBy("date_only")

profTemp.show(5)

profTemp.coalesce(1).write.option("header", true).mode("overwrite").csv("final/cleaned/weather/agg")
sodResult.coalesce(1).write.option("header", true).mode("overwrite").csv("final/cleaned/weather/cond")

