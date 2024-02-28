package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession

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



  }

}