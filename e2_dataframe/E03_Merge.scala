package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.{SaveMode, SparkSession}

object E03_Merge {

  def main(args: Array[String]) {

    /**
      *  Notre startup est mytube connait une augmentation exponentielle du trafic,
      *  l'equipe marketing, nous as chargé d'étudier le dataset des filliales Us et GB.
      *  Mais avant de commencer l'étude, on doit fusioner les deux datasets:
      *  - Chargez le fichier USvideos.csv
      *  - Faire un show sur ce Dataset pour explorer les données
      *  - C'est quoi son schema ?
      *  - Essayez de forcer Spark à déduire le bon schéma
      *  - Chargez le fichier GBvideos.csv avec avec les mêmes paramètres que le USvideos.csv
      *  - Fussionez les deux datasets dans un fichier videos.parquet au format parquet ?
      *  - À quoi ressemble le fichier videos.parquet ?
      *  - Lancez ce job plusieurs fois en jouant  avec le SaveMode, pour écrasez le fichier à chaque lancement
      *
      * */
    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-3")
      .getOrCreate()


    val usVideos = sparkSession.read
      .option("header","true")
      .csv("src/main/resources/USvideos.csv")

    usVideos.show()
    usVideos.printSchema()

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
    videos.write
      .option("header", "true")
      .mode("overwrite")
      .csv("path_to_save/videos.csv")
    /**
     * JAI UNE ERREUR ICI LIEE A HADOOP
     */
    videos.write.mode("overwrite").parquet("src/main/resources/videos.parquet")


    val videosParquet = sparkSession.read.parquet("src/main/resources/videos.parquet")

    videosParquet.show()
    videosParquet.printSchema()
    /**
      * Faire de meme pour le dataset comments.csv
      * */
    val comments = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/comments.csv")

    comments.show()
    comments.printSchema()

    sparkSession.close()

  }

}
