package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession

object E06_Sort {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-3")
      .getOrCreate()

    import sparkSession.implicits._
    /**
      * En utilisant la fonction sort, retrouvez:
      *   - la liste des  5 vidéos  les plus consultées, avec le nombre de vu
      *   - la liste des  5 vidéos  les moin consultées, avec le nombre de vu
      *   - La video qui a le plus grand nombre de like
      *   - La video qui a le plus grand nombre de dislike
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

    val top5MostViewedVideos = videos
      .orderBy($"views".desc)
      .select($"title", $"views")
      .limit(5)

    top5MostViewedVideos.show()

    val top5LeastViewedVideos = videos
      .orderBy($"views".asc)
      .select($"title", $"views")
      .limit(5)

    top5LeastViewedVideos.show()

    val mostLikedVideo = videos
      .orderBy($"likes".desc)
      .select($"title", $"likes")
      .limit(1)

    mostLikedVideo.show()

    val mostDislikedVideo = videos
      .orderBy($"dislikes".desc)
      .select($"title", $"dislikes")
      .limit(1)

    mostDislikedVideo.show()

    sparkSession.close()

  }

}
