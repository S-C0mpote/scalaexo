package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession

object E06_Sort {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[1]")
      .appName("exo-3")
      .getOrCreate()

    import sparkSession.implicits._
    /**
      * En utilisant la fonction sort, retrouvez:
      *   - la liste des  5 vidéos  les plus consultées, avec le nombre de vu
      *   - la liste des  5 vidéos  les moin consultées, avec le nombre de vu
      *   - La video qui a le plus grand nombre de like
      *   - La video qui a le plus grand nombre de dislike
      * */

    sparkSession.close()

  }

}
