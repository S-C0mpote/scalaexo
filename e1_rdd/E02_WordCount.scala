package com.fabulouslab.spark.e1_rdd

import org.apache.spark.sql.SparkSession

object E02_WordCount {

  def main(args: Array[String]) {

    /**
      *   Ici on veut:
      *   - Compter le nombre de mots
      *   - le nombre d'occurrences de chaque mot dans le texte, en utilisant reduceByKey et groupByKey
      *   - C'est quoi l'impact sur le temps de traitement ?
      *   - Mesurez le shuffle dans les deux cas avec Spark UI?
      * */
    val sparkSession = SparkSession.builder
      .master("local[4]")
      .appName("exo-2")
      //.config("spark.default.parallelism", "22")
      .getOrCreate()

    import sparkSession.implicits._
    val data = sparkSession.read.text("src/main/resources/kinglear.txt").as[String].rdd.persist()
    val words = data.flatMap(line => line.split("\\s+"))
    val totalWords = words.count()
    println(s"Nombre total de mots: $totalWords")


    //Thread.sleep(1000000)
    sparkSession.close

  }

}
