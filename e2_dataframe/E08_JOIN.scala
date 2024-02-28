package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object E08_JOIN {

  def main(args: Array[String]) {

    /**
      *   - Charger le fichier categories.json avec le format adéquat
      *   - Est-il un fichier Json valid ? pour quoi ?
      * En utilisant  join :
      *   - calculer le nombre de commentaires par vidéo
      *   - calculer le nombre de commentaires par catégorie
      * */
    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-8")
      .getOrCreate()

    import sparkSession.implicits._
    val categoriesDF = sparkSession.read.json("src/main/resources/categories.json")

    categoriesDF.printSchema()
    //Aucun probleme de lecture ou exception, le fichier est bien valide

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

    val commentCountPerVideo = videos
      .groupBy("video_id")
      .agg(sum("comment_total").as("totalComments"))
      .orderBy($"totalComments".desc)

    commentCountPerVideo.show()

    val commentCountPerCategory = videos
      .join(categoriesDF, videos("category_id") === categoriesDF("id"))
      .groupBy(categoriesDF("title"))
      .agg(sum("comment_total").as("totalComments"))
      .orderBy($"totalComments".desc)

    commentCountPerCategory.show()


  }

}