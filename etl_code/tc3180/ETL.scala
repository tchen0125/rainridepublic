import org.apache.spark.sql.functions._

val file_path = "/user/bj2351_nyu_edu/final/data/motor-vehicle-collisions-2022.csv"
val data = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

// Date Formatting
val dataFormatted = data.withColumn("CRASH DATE", to_date(col("CRASH DATE"), "yyyy-MM-dd"))

// Text formatting
val dataClean = dataFormatted.withColumn("BOROUGH", upper(trim(col("BOROUGH"))))
dataClean.show()

// Calculate collision counts
val collisionCounts = dataClean.groupBy("CRASH DATE").count().withColumnRenamed("count", "Crash Number")
collisionCounts.show()
val numericalColumns = Seq(
  "NUMBER OF PERSONS INJURED", "NUMBER OF PERSONS KILLED",
  "NUMBER OF PEDESTRIANS INJURED", "NUMBER OF PEDESTRIANS KILLED",
  "NUMBER OF CYCLIST INJURED", "NUMBER OF CYCLIST KILLED",
  "NUMBER OF MOTORIST INJURED", "NUMBER OF MOTORIST KILLED"
)

// Prepare aggregated column expressions
val sumExpressions = numericalColumns.map(c => sum(col(c)).alias(c + "_sum"))

// Aggregate the daily sums for each numerical column
val aggregatedData = dataClean.groupBy("CRASH DATE").agg(sumExpressions.head, sumExpressions.tail: _*)

// Join with collisionCounts
val combinedData = collisionCounts.join(aggregatedData, "CRASH DATE")
combinedData.show()


// Save dataClean, combinedData, and aggStats to HDFS
dataClean.coalesce(1).write.option("header", "true").csv("/user/bj2351_nyu_edu/final/cleaned/motor-vehicle-collision")
combinedData
.orderBy("CRASH DATE")
.coalesce(1)
.write
.option("header", "true")
.csv("/user/bj2351_nyu_edu/final/cleaned/collisionData")


