package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.{SensorEvent, SensorKeyObject, TwoWorkEvents}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

class WorkValueProcessWindow extends ProcessWindowFunction[SensorEvent, TwoWorkEvents, SensorKeyObject, GlobalWindow] {
  override def process(key: SensorKeyObject, context: Context, elements: Iterable[SensorEvent], out: Collector[TwoWorkEvents]): Unit = {

    elements match {
      case x: Iterable[SensorEvent] if x.size == 2 => out.collect(TwoWorkEvents(x.toList(0), x.toList(1)))
      case _ => println("Encountered non-pair CountWindow Work")
    }
  }
}
