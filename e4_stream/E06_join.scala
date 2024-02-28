package com.fabulouslab.spark.e04_stream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


case class User(key : String, name : String)


object E06_Join {


  def main(args: Array[String]) {

    /**
     *   getUserDs est une fonction qui retourne un dataframe,
     *   qui contient id du user(= key) et son nom ( voir la classe User).
     *    - En faisant une jointure entre le stream, et le dataset[User],
      *    affichez dans la console, le noms du user et le lien sur lequel il a cliqué
      *  * */
    

    implicit val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-3")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    import sparkSession.implicits._

    val users = getUserDS



  }

  def getUserDS(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
    (0 to 10)
      .map(i => User(s"User_$i",s"nom$i")).toDF()
  }

}
