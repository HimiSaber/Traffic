package com.utils

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object String2DateTime {

  private val df: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHH")

  def get2Hour(time: String): DateTime={
    df.parseDateTime(time)
  }

}
