import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types._

object mySparkJob{
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Job")
      .getOrCreate()

    val statsSchema = StructType(
      Array(
        StructField("date", TimestampType),
        StructField("game_size", IntegerType),
        StructField("match_id", StringType),
        StructField("match_mode", StringType),
        StructField("party_size", IntegerType),
        StructField("player_assists", IntegerType),
        StructField("player_dbno", IntegerType),
        StructField("player_dist_ride", DecimalType(9,2)),
        StructField("player_dist_walk", DecimalType(9,2)),
        StructField("player_dmg", IntegerType),
        StructField("player_kills", IntegerType),
        StructField("player_name", StringType),
        StructField("player_survive_time", FloatType),
        StructField("team_id", StringType),
        StructField("team_placement", IntegerType)
      )
    )



    val hdfspath = "hdfs:///raw/*.csv"
    val statsDF = spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
      .schema(statsSchema)
      //.csv("s3://airflow-test-buck-20072022/PUBGMatchDeathsAndStatistics/agg_match_stats_1.csv")
      //.csv("hdfs:///raw/2022-07-30.csv")
      .csv(hdfspath)

    import spark.implicits._
    val killRateDF = statsDF
      .drop($"party_size")
      .withColumn("kill_rate",
        (($"player_kills"/$"player_survive_time")*60).cast(DecimalType(4, 2)))

    val catDF = killRateDF
      .withColumn("player_kills_cat",
        when($"player_kills" === 0, "none")
          .when($"player_kills".between(1, 3), "low")
          .when($"player_kills".between(4, 6), "medium")
          .when($"player_kills".between(6, 8), "high")
          .otherwise("very high"))
      .orderBy($"player_kills".desc)

    catDF.write.parquet("hdfs:///processed/")

//    println("Finding date key:...")
//    val pathDF = catDF.select(input_file_name()).distinct().collect().mkString("")
//    val regex_pattern = """(\d{4})-(\d{2})-(\d{2})""".r
//    val extractedDate = regex_pattern.findFirstIn(pathDF).mkString("")
//    println(extractedDate)
//
//    catDF.write.mode("append").parquet(s"s3://aggrjobout/$extractedDate.parquet")
    //catDF.write.parquet("s3://2022-stats-parquets/parquets/")

  }
}