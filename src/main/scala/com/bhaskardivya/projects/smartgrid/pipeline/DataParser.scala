package com.bhaskardivya.projects.smartgrid.pipeline

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

object DataParser {
  def parse(stream: DataStream[String]): DataStream[List[String]] = {
    stream
      .map { line =>
        line.split("\\s+").toList
      }
  }
}
