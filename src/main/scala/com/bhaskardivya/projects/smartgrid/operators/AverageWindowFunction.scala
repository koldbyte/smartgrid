package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.Average
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AverageWindowFunction extends WindowFunction[Average, (Long, Average), Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Average], out: Collector[(Long, Average)]): Unit = {
    val avg: Average = Average(0.0, 0L)
    for(elements <- input){
      avg.add(elements)
    }

    out.collect((key, avg))
  }
}
