package com.fabulouslab.spark.e04_stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, from_json}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object E03_filter {

  case class View(viewtime: Long, userid: String, pageid: String)
  def main(args: Array[String]) {

    /**
      *   Toujours sur le stream du topic pageviews :
      *   - Construire un stream qui contient que les events sur la page 3
      *   - Construire un stream qui contient que les events sur le user 3
      *   - Construire un stream qui contient que les events sur le user 3 avec un temps de visite supérieur à 2 secondes
      *   - Réinjecter ces streams dans un topic Kafka
      * */
    

  }

}
