package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object E10_UDF {

  def main(args: Array[String]) {

    /**
      * User-Defined Functions (UDF) est une fonctionnalité de SparkSQl qui permet de définir une nouvelle  colonne
      * avec une fonction  créée par l'utilisateur, ainsi ça permet d'erichir le DSL de base.
      *
      *   - Créez une fonction qui prend en paramètre un string et retourne le nombre de mots séparés par des pipes :
      *      ex : toto|titi|tonton => 3
      *   - Enregistrez cette fonction avec la fonction udf
      *   - Créez une nouvelle colonne avec le nombre de tag par video
      * */

    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-9")
      .getOrCreate()

    import sparkSession.implicits._


  }

}


