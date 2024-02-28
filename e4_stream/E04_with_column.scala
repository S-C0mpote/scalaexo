package com.fabulouslab.spark.e04_stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, from_json}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object E04_with_column {

  def main(args: Array[String]) {

    /**
      * La fonction withColumn permet de créer un nouveau dataframe en ajoutant une nouvelle colonne,
      * elle est souvent associée à des fonctions dans le package org.apache.spark.sql.functions.
      * En utilisant withColumn:
      *   - Enrichir le stream en ajoutant 2 colonnes :
      *      - La date de traitement de l'event
      *      - Le temps passé sur la page en second
      * */


  }

}
