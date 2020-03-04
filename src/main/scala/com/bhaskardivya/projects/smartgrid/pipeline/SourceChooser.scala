package com.bhaskardivya.projects.smartgrid.pipeline

import com.bhaskardivya.projects.smartgrid.model.SensorEvent
import com.bhaskardivya.projects.smartgrid.sources.{FileSource, KafkaSource}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Reads 'source' parameter from command line and chooses the correct source for stream input
  */
object SourceChooser {
  def from(env: StreamExecutionEnvironment, params: ParameterTool): DataStream[SensorEvent] = {
    val source = params.get("source", "kafka")

    source match {
      case "kafka" => new KafkaSource().getSource(env, params)
      case "file" => new FileSource().getSource(env, params)
      case "simulated" => new FileSource().getSimulatedCSVSource(env, params)
      case _ => new FileSource().getSource(env, params)
    }
  }
}
