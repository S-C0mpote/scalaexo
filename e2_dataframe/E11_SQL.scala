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

    val videos = sparkSession.read
      .option("header", "true")
      .csv("src/main/resources/USvideos.csv")
    import sparkSession.implicits._

  }

}
