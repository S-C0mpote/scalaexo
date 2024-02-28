package com.fabulouslab.spark.e3_dataset


import java.time.LocalDate
import java.util.Date

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import com.fabulouslab.spark.utils.DateUtils

object E02_Dedouble {

  def getLastVideo(v1: Video, v2: Video) : Video= {
    val v1Date = DateUtils.toDate(v1.date).getOrElse(LocalDate.MIN)
    val v2Date = DateUtils.toDate(v2.date).getOrElse(LocalDate.MIN)
    if(v1Date.isAfter(v2Date))
      v1
    else
      v2
  }

  def main(args: Array[String]) {

    /**
      * - En réalité video.csv est une série temporelle à la journée. En réutilisant  le Dataset[Video] crée dans
      *   l'exercice précédant, gardez dans le dataset que le dernier événement reçu pour chaque vidéo.
      *   Indication : utilisez les fonctions groupByKey et reduceGroups
      * */

    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-1")
      .getOrCreate()
    import sparkSession.implicits._
    object DateUtils {
      def toDate(dateStr: String): Option[LocalDate] = Some(LocalDate.now()) // Implémentation fictive
    }

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

    val videoDS: Dataset[Video] = videos.as[Video]

    val latestVideoDS = videoDS
      .groupByKey(_.video_id)
      .reduceGroups((v1, v2) => getLastVideo(v1, v2))
      .map(_._2)
    latestVideoDS.show()


    sparkSession.close()

  }
}
