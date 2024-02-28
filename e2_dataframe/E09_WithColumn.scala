package com.fabulouslab.spark.e2_dataframe


import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object E09_WithColumn {

  def main(args: Array[String]) {

    /**
      *   La fonction withColumn permet de créer un nouveau dataframe en ajoutant une nouvelle colonne,
      *   elle est souvent associée à des fonctions de org.apache.spark.sql.functions.
      *   En utilisant withColumn:
      *   - Créez une nouvelle column popularité (popularité =  nombre de likes -  nombre de dislikes)
      *   - Créez une nouvelle column en formatant correctement la date
      *   - Créez une nouvelle column id, ou id est le hash sha1 de video_id
      **/
    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-9")
      .getOrCreate()

    import sparkSession.implicits._

    sparkSession.close()

  }

}
