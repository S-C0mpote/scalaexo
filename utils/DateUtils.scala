package com.fabulouslab.spark.utils

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.util.Try

object DateUtils {

  val MYTUBE_DATE_FORMAT = DateTimeFormatter.ofPattern("d.MM.yyyy")


  def toDate( date : String) = Try {

    LocalDate.parse(date + ".2019", MYTUBE_DATE_FORMAT)
  }
}
