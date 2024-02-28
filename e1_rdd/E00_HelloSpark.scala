package com.fabulouslab.spark.e1_rdd

import org.apache.spark.sql.SparkSession


object E00_HelloSpark {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[4]")
      .appName("exo-0")
      .getOrCreate()

    if(!sparkSession.sparkContext.isStopped){
      println("Spark est OK !")
    }
    else{
      println("Spark est KO!")
    }


    sparkSession.close
  }

}
