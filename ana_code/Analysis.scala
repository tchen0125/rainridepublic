import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions._

val configuration = new Configuration()
val fileSystem = FileSystem.get(configuration)

def getFilePath(path: Path): String =  {
    val file = fileSystem
        .listStatus(path)
        .filter(_.getPath.getName.endsWith(".csv"))

    file.head.getPath.toString
}

// Data Preparation

val cleanedPath = "/user/bj2351_nyu_edu/final/cleaned/"
val weatherAggPath = getFilePath(new Path(cleanedPath + "weather/agg"))
val weatherCondPath = getFilePath(new Path(cleanedPath + "weather/cond"))
val collisionPath = getFilePath(new Path(cleanedPath + "collisionData"))
val ridershipPath = getFilePath(new Path(cleanedPath + "daily-ridership"))

val csvOptions = Map("header" -> "true", "inferSchema" -> "true")

val weatherAgg = (
  spark.read.options(csvOptions).csv(weatherAggPath)
    .withColumnRenamed("date_only", "date")
    .withColumnRenamed("MinTemp", "min t")
    .withColumnRenamed("MaxTemp", "max t")
    .withColumn("AvgTemp", round($"AvgTemp", 1))
    .withColumnRenamed("AvgTemp", "avg t")
)


val weatherCond = (
  spark.read.options(csvOptions).csv(weatherCondPath)
    .drop("DATE")
    .withColumnRenamed("date_only", "date")
)
val collision   = (
  spark.read.options(csvOptions).csv(collisionPath)
  .withColumnRenamed("CRASH DATE", "date")
  .withColumnRenamed("CRASH Number", "crash")
  .withColumnRenamed("NUMBER OF PERSONS INJURED_sum", "ppl i")
  .withColumnRenamed("NUMBER OF PERSONS KILLED_sum",  "ppl k")
  .withColumnRenamed("NUMBER OF PEDESTRIANS INJURED_sum", "ped i")
  .withColumnRenamed("NUMBER OF PEDESTRIANS KILLED_sum", "ped k")
  .withColumnRenamed("NUMBER OF CYCLIST INJURED_sum", "cyc i")
  .withColumnRenamed("NUMBER OF CYCLIST KILLED_sum", "cyc k")
  .withColumnRenamed("NUMBER OF MOTORIST INJURED_sum", "mot i")
  .withColumnRenamed("NUMBER OF MOTORIST KILLED_sum", "mot k")
)

val ridership   = (
  spark.read.options(csvOptions).csv(ridershipPath)
    .withColumnRenamed("Date", "date")
    .withColumnRenamed("Subways: Total Estimated Ridership", "sub")
    .withColumnRenamed("Buses: Total Estimated Ridership", "bus")
    .withColumnRenamed("LIRR: Total Estimated Ridership", "lirr")
    .withColumnRenamed("Metro-North: Total Estimated Ridership", "metro-north")
    .withColumnRenamed("Access-A-Ride: Total Scheduled Trips", "acc-a-ride")
    .withColumnRenamed("Bridges and Tunnels: Total Traffic", "brdg-tun")
    .withColumnRenamed("Staten Island Railway: Total Estimated Ridership", "sttn-rw")
)

val full = weatherAgg.join(weatherCond, "date").join(collision, "date").join(ridership, "date")

full.cache()

// Data Analysis

full.show()

// Does a single weather condition have something to do with other numbers?

val conditions = List("Rain", "Snow", "Fog", "Haze", "Mist")

println("Crashes and Ridership on each Weather Condition")
val conditionAnalysis = conditions.map(cd => (cd, {
  full
    .groupBy(col(cd))
    .agg(
      count("crash").alias("days"),
      sum("crash").alias("tt crash"),
      sum("ppl i").alias("ppl injured"),
      sum("ppl k").alias("ppl killed"),
      sum("sub").alias("sub"),
      sum("bus").alias("bus")
    )
    .withColumn("ppl injured per day", round($"ppl injured" / $"days", 3))
    .withColumn("ppl killed per day", round($"ppl killed" / $"days", 3))
    .withColumn("crashes per day", round($"tt crash" / $"days"))
    .withColumn("sub per day", round(col("sub") / col("days")))
    .withColumn("bus per day", round(col("bus") / col("days")))
})).toMap


conditionAnalysis.foreach({ case (k, v) => v.show() })

val weatherColumns = List("min t", "max t", "avg t", "Sunrise", "Sunset")
val collAndTransportCols = List("crash", "ppl i", "ppl k", "ped i", "ped k", "cyc i", "cyc k", "mot i", "mot k", "sub", "bus", "lirr", "metro-north", "acc-a-ride", "brdg-tun", "sttn-rw")

println("Pearson Correlation between weather and traffic data")


weatherColumns.foreach(w => {
  collAndTransportCols.foreach(d => {
    val corr = full.stat.corr(w, d)
    println(s"$w, $d -> $corr")
  })
})

full.unpersist()


// Feature Engineering
val assembler = new VectorAssembler().setInputCols(Array("min t", "max t", "sub", "bus")).setOutputCol("features")

val transformedData = assembler.transform(full)

val Array(trainingData, testData) = transformedData.randomSplit(Array(0.7, 0.3))

val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("crash")

val lrModel = lr.fit(trainingData)

val predictions = lrModel.transform(testData)

println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

val trainingSummary = lrModel.summary

println(s"R-squared: ${trainingSummary.r2}")
println(s"T-values: ${trainingSummary.tValues.mkString(", ")}")


// Define the new Linear Regression model for 'ppl injured'
val lrPplInjured = new LinearRegression().setFeaturesCol("features").setLabelCol("ppl i")

// Fit the model to the training data
val lrModelPplInjured = lrPplInjured.fit(trainingData)

// Make predictions on the test data
val predictionsPplInjured = lrModelPplInjured.transform(testData)

// Print the coefficients and intercept for the new model
println(s"Model for 'ppl injured'")
println(s"Coefficients: ${lrModelPplInjured.coefficients} Intercept: ${lrModelPplInjured.intercept}")

// Get the summary for the new model
val trainingSummaryPplInjured = lrModelPplInjured.summary

println(s"R-squared for 'ppl injured': ${trainingSummaryPplInjured.r2}")
println(s"T-values for 'ppl injured': ${trainingSummaryPplInjured.tValues.mkString(", ")}")

val lrPplKilled = new LinearRegression().setFeaturesCol("features").setLabelCol("ppl k")

val lrModelPplKilled = lrPplKilled.fit(trainingData)

// Make predictions on the test data
val predictionsPplKilled = lrModelPplKilled.transform(testData)

// Print the coefficients and intercept for the new model
println(s"Model for 'ppl k'")
println(s"Coefficients: ${lrModelPplKilled.coefficients} Intercept: ${lrModelPplKilled.intercept}")

// Get the summary for the new model
val trainingSummaryPplKilled = lrModelPplKilled.summary

// Print R-squared and T-values for the new model
println(s"R-squared for 'ppl k': ${trainingSummaryPplKilled.r2}")
println(s"T-values for 'ppl k': ${trainingSummaryPplKilled.tValues.mkString(", ")}")
