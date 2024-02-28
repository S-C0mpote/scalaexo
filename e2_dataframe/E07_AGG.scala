package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object E07_AGG {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("exo-3")
      .getOrCreate()

    import sparkSession.implicits._
    /**
      * En utilisant la fonction groupBy :
      * - trouvez le nombre de vidéos par channel ?
      * - le max de views par channel ?
      * - nombre de vidéos par channel et catégorie  ?
      * - la catégorie  la plus regardé ?
      * - la catégorie  la moins regardé ?
      * - le nombre de de vidéos qui n'ont pas été regardées et le nombre de vidéos qui ont
      *   plus que 10000 views en une seule passe ( utilisez la fonction viewToTuple)
      * */
    val usVideosWithSchema = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/USvideos.csv")

    usVideosWithSchema.printSchema()

    val gbVideos = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/GBvideos.csv")

    val videos = usVideosWithSchema.union(gbVideos)

    val videosPerChannel = videos
      .groupBy($"channel_title")
      .count()
      .orderBy($"count".desc)

    videosPerChannel.show()

    val maxViewsPerChannel = videos
      .groupBy($"channel_title")
      .max("views")
      .orderBy($"max(views)".desc)

    maxViewsPerChannel.show()

    val videosPerChannelAndCategory = videos
      .groupBy($"channel_title", $"category_id")
      .count()
      .orderBy($"count".desc)

    videosPerChannelAndCategory.show()

    val mostWatchedCategory = videos
      .groupBy($"category_id")
      .sum("views")
      .withColumnRenamed("sum(views)", "totalViews")
      .orderBy($"totalViews".desc)
      .limit(1)

    mostWatchedCategory.show()

    val leastWatchedCategory = videos
      .groupBy($"category_id")
      .sum("views")
      .withColumnRenamed("sum(views)", "totalViews")
      .orderBy($"totalViews".asc)
      .limit(1)

    leastWatchedCategory.show()

    // Définir la UDF
    val viewToTupleUDF = udf(viewToTuple _)

    // Appliquer la UDF et agréger
    val videoViewsStats = videos
      .withColumn("viewsTuple", viewToTupleUDF($"views"))
      .select($"viewsTuple._1".as("notWatched"), $"viewsTuple._2".as("moreThan10000Views"))
      .groupBy()
      .sum("notWatched", "moreThan10000Views")

    videoViewsStats.show()

    sparkSession.close()

  }
  def viewToTuple(view: Int) = {
    if (view == 0)
      (1, 0)
    else if (view > 1000)
      (0, 1)
    else
      (0, 0)
  }

}
