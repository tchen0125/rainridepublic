import org.apache.spark.sql.functions._
import java.sql.Date

val inputFile = "/user/bj2351_nyu_edu/final/data/MTA-daily-ridership.csv"
val dailyRidershipDF = spark.read.option("header", true).csv(inputFile)

val columnsToDrop = Seq(
  "Subways: % of Comparable Pre-Pandemic Day",
  "Buses: % of Comparable Pre-Pandemic Day",
  "LIRR: % of Comparable Pre-Pandemic Day",
  "Metro-North: % of Comparable Pre-Pandemic Day",
  "Access-A-Ride: % of Comparable Pre-Pandemic Day",
  "Bridges and Tunnels: % of Comparable Pre-Pandemic Day",
  "Staten Island Railway: % of Comparable Pre-Pandemic Day"
)

val cleanedDF = dailyRidershipDF.drop(columnsToDrop: _*)

val cleanDF = cleanedDF.withColumn("Date", to_date($"Date", "MM/dd/yyyy"))

val filteredDF = cleanDF.filter(col("Date").between(startDate, endDate))


val startDate = Date.valueOf("2022-01-01")
val endDate = Date.valueOf("2022-12-31")

val filteredDF = cleanDF.filter(col("Date").between(startDate, endDate))

filteredDF.show()

filteredDF.write.option("header", "true").csv("/user/lt2205_nyu_edu/daily-ridership")
filteredDF.write.option("header", "true").csv("/user/bj2351_nyu_edu/final/cleaned/daily-ridership")

