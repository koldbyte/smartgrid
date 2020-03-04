package com.bhaskardivya.projects.smartgrid.pipeline

import org.apache.flink.streaming.api.scala._

object DataCleanser {
  def clean(stream: DataStream[List[String]]): DataStream[List[String]] = {
    stream
      .filter(_.lengthCompare(7) == 0) // There should be 7 fields in a record
      .filter(row => row(3).toInt == 0 && row(3).toInt == 1) // 'property' can have either 0 or 1 value
  }
}
