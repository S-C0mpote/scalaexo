package com.fabulouslab.spark.e04_stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.functions.from_json



object E01_hello_kafka {

  def main(args: Array[String]) {

    /**
      * On reçoit en temps réel via Kafka sur le topic pageviews, un stream du comportement des visiteurs
      * sur un site internet
      *   - Afficher le contenu de ce stream dans la console.
      *   - Afficher la clé et la valeur dans la console (format String), en streamant le topic pageviews
      * */

  }

}
