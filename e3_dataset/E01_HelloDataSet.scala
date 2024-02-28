package com.fabulouslab.spark.e3_dataset


import org.apache.spark.sql.{SaveMode, SparkSession}
case class Video( video_id: String, title: String, channel_title : String, category_id : Int, tags: String, views : Int , likes : Long, dislikes :Int, comment_total :Int, thumbnail_link : String, date : String)

object E01_HelloDataSet {

  def main(args: Array[String]) {

    /**
      * - Créez une case class VideoRaw (avec tout les attributs en string ) qui  modélise un row du fichier video.csv
      * - Créez une case class Video qui  modélise un row du même dataframe avec le bon typage
      * - Chargez le video.csv dans un dataframe sans inference de schéma
      * - Transformez ce Dataframe en un Dataset de VideoRaw puis en Dataset de Video
      * - En utilisant le fonction groupByKey avec la signature groupByKey[K: Encoder](func: T => K),
      *   calculez , le nombre de vidéos par catégorie
      * */

    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-1")
      .getOrCreate()
    import sparkSession.implicits._

    sparkSession.close()

  }

}

