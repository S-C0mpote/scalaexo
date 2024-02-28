package com.fabulouslab.spark.e04_stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class E07_watermark {

  def main(args: Array[String]) {

    /**
      *   - Streamer les topic users, et pageviews avec un watermark d'une minute (utiliser la colonne timestamp).
      *   - Faire la jointure entre ces deux streams.
      *   - Calculer le nombre de visite par région chaque minute.
      * */

  }

}
