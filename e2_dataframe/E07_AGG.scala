package com.fabulouslab.spark.e2_dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object E07_AGG {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("exo-3")
      .getOrCreate()

    import sparkSession.implicits._
    /**
      * En utilisant la fonction groupBy :
      * - trouvez le nombre de vidéos par channel ?
      * - le max de views par channel ?
      * - nombre de vidéos par channel et catégorie  ?
      * - la catégorie  la plus regardé ?
      * - la catégorie  la moins regardé ?
      * - le nombre de de vidéos qui n'ont pas été regardées et le nombre de vidéos qui ont
      *   plus que 10000 views en une seule passe ( utilisez la fonction viewToTuple)
      * */

    sparkSession.close()

  }
  def viewToTuple(view: Int) = {
    if (view == 0)
      (1, 0)
    else if (view > 1000)
      (0, 1)
    else
      (0, 0)
  }

}
