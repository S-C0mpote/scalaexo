package com.fabulouslab.spark.e04_stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, from_json}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.OutputMode.Complete
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object E05_agg {

  def main(args: Array[String]) {

    /**
      *   - Compter le temps moyen passé sur une page,
      *   - Compter le temps max passé sur une page,
      *   - Compter en une seule fois le temps moyen et max passé sur une page.
      * */

  }

}
