package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object E11_SQL {

  def main(args: Array[String]) {

    /**
      *   En utilisant les fonctions En utilisant les fonctions stddev et corr de SparkSQL :
      *   - Calculez la deviation standard  de nombre de commentaires.
      *   - Calculez la corrélation entre le nombre de commentaires et le nombre de likes.
      *   - Enregistrez le dataframe dans une table via la fonction createOrReplaceTempView
      *   - En utilisant sparkSession.sql, il faut interroger la table via sql
      **/

    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-9")
      .getOrCreate()

    val videosUS = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/USvideos.csv")
    import sparkSession.implicits._

    val videos = videosUS
      .withColumn("comment_total", $"comment_total".cast("int"))
      .withColumn("likes", $"likes".cast("int"))

    val stddevCommentCount = videos.agg(stddev($"comment_total").alias("stddevCommentTotal"))
    stddevCommentCount.show()

    val correlationCommentLikes = videos.agg(corr($"comment_total", $"likes").alias("corrCommentLikes"))
    correlationCommentLikes.show()

    videos.createOrReplaceTempView("videos_view")

    val stddevCommentCountSQL = sparkSession.sql("SELECT stddev(comment_total) as stddevCommentCount FROM videos_view")
    stddevCommentCountSQL.show()

    val correlationCommentLikesSQL = sparkSession.sql("SELECT corr(comment_total, likes) as corrCommentLikes FROM videos_view")
    correlationCommentLikesSQL.show()
  }

}
