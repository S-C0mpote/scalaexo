package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession

object E05_Filtering2 {

  def main(args: Array[String]) {

    /**
      * Maintenant, on va essayer de faire des filtres plus compliqués :
      *   - trouvez la liste des vidéos dans le channel MTV et le titre contient "Hand In Hand"
      *   - trouvez le nombre de vidéos avec un id catergory 28 ou aucun commentaire ?
      * */

    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-5")
      .getOrCreate()

    import sparkSession.implicits._
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

    val mtvHandInHandVideos = videos.filter(
      $"channel_title" === "MTV" && $"title".contains("Hand In Hand")
    )

    mtvHandInHandVideos.show()


    val videosCategory28OrNoComments = videos.filter(
      $"category_id" === 28 || $"comment_total" === 0
    ).count()

    println(s"Nombre de vidéos avec categoryId 28 ou aucun commentaire: $videosCategory28OrNoComments")
    sparkSession.close()

  }

}
