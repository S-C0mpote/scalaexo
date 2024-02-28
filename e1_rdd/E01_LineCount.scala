package com.fabulouslab.spark.e1_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object E01_LineCount {

   def main(args: Array[String]) {

     /**
      *    Le but est de compter le nombre de lignes dans un fichier texte (kinglear.txt), pour cela, on va utiliser les RDD Spark
       *   et voila les étapes pour y parvenir :
       *   - Création de la session Spark.
       *   - Le chargement du fichier dans un RDD[String].
       *   - Comptez le nombre de lignes.
       *   - Le fichier contient des lignes vides, comptez le nombre de ligne vide et le nombre de lignes non vide
       *
      */
    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-1")
      .getOrCreate()

    import sparkSession.implicits._

     /*
       Le code suivant va charger un ficher dans un RDD de String, depuis la session Spark
     */
     val kinglear: RDD[String] = sparkSession.sparkContext.textFile("src/main/resources/kinglear.txt")

     /**
        Ce fichier contient combien de lignes  ? Affichez le resultat dans la console
     */
      println(kinglear.count())
     /**
        Filtrez les lignes vides, combien le roman contient de lignes non vides ?
     */
     println(kinglear.filter(line => line.trim.nonEmpty).count())


     Thread.sleep(5000)

     sparkSession.close

  }

}
