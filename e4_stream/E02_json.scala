package com.fabulouslab.spark.e04_stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}


object E02_json {

  def main(args: Array[String]) {

    /**
      *   Toujours sur le flux du topic pageviews :
      *   - Déserialiser la value json en Struct
      *   - Construire un dataframe depuis les attributs du Struct
      * */

  }

}
