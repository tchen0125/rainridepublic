import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

val configuration = new Configuration()
val fileSystem = FileSystem.get(configuration)


def getFilePath(path: Path): String =  {
  val file = fileSystem
      .listStatus(path)
      .filter(_.getPath.getName.endsWith(".csv"))
  file.head.getPath.toString
}

val ridershipDirectory = "/user/bj2351_nyu_edu/final/cleaned/daily-ridership"
val weatherDirectory = "/user/bj2351_nyu_edu/final/cleaned/weather/cond"

val ridershipPath = getFilePath(new Path(ridershipDirectory))
val weatherPath = getFilePath(new Path(weatherDirectory))

val ridershipDF = spark.read.option("header", true).csv(ridershipPath)
val weatherDF = spark.read.option("header", true).csv(weatherPath)

val joinedDF = ridershipDF.join(weatherDF, ridershipDF("Date") === weatherDF("date_only"))


val transitColumns = Seq(
            "Subways: Total Estimated Ridership",
            "Buses: Total Estimated Ridership",
            "Bridges and Tunnels: Total Traffic",
     )
     
transitColumns.foreach { transitColumn =>
      val numericColumn = col(transitColumn).cast(DoubleType)

      // Calculate means and standard deviations for each transit option based on raining
      val rainAnalysisResult = joinedDF.groupBy("Rain")
        .agg(
          mean(when(col(transitColumn) =!= "", numericColumn)).alias(s"${transitColumn}_mean_rain"),
          stddev(when(col(transitColumn) =!= "", numericColumn)).alias(s"${transitColumn}_stddev_rain")
        )

      // Calculate means and standard deviations for each transit option based on snowing
      val snowAnalysisResult = joinedDF.groupBy("Snow")
        .agg(
          mean(when(col(transitColumn) =!= "", numericColumn)).alias(s"${transitColumn}_mean_snow"),
          stddev(when(col(transitColumn) =!= "", numericColumn)).alias(s"${transitColumn}_stddev_snow")
        )

      // Calculate means and standard deviations for each transit option when neither raining nor snowing
      val neitherAnalysisResult = joinedDF.filter(col("Rain") === "false" && col("Snow") === "false")
        .agg(
          mean(when(col(transitColumn) =!= "", numericColumn)).alias(s"${transitColumn}_mean_neither"),
          stddev(when(col(transitColumn) =!= "", numericColumn)).alias(s"${transitColumn}_stddev_neither")
        )

      // Show the analysis result for raining
      println(s"\nAnalysis for $transitColumn based on Raining:")
      rainAnalysisResult.show()

      // Show the analysis result for snowing
      println(s"\nAnalysis for $transitColumn based on Snowing:")
      snowAnalysisResult.show()

      // Show the analysis result when neither raining nor snowing
      println(s"\nAnalysis for $transitColumn when Neither Raining nor Snowing:")
      neitherAnalysisResult.show()
    }
