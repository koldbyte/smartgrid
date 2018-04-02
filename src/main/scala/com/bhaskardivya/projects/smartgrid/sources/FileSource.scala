package com.bhaskardivya.projects.smartgrid.sources

import com.bhaskardivya.projects.smartgrid.model.SensorEvent
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


class FileSource {
  def getSimulatedCSVSource(env: StreamExecutionEnvironment, params: ParameterTool): DataStream[SensorEvent] = {
    val data = params.get("input", "/data/data.gz")
    val maxServingDelay = params.getInt("maxServingDelay", 0)
    val servingSpeedFactor = params.getFloat("servingSpeedFactor", 1f)
    val offsetEventTimestamp = params.has("offsetEventTimestamp")

    val events = env.addSource(new CSVFileSource(data, maxServingDelay, servingSpeedFactor, offsetEventTimestamp))
      .name("CSV GZ File")

    events
  }

  def getSource(env: StreamExecutionEnvironment, params: ParameterTool): DataStream[SensorEvent] = {
    val filePath: String = params.get("input", "/data/data.gz")

    // read the CSV GZ and assign Timestamp
    val csv: DataStream[SensorEvent] = env.readTextFile(filePath)
      .map[SensorEvent](line => SensorEvent.fromString(line))
      .assignTimestampsAndWatermarks(SensorEvent.tsAssigner())

    csv
  }

}
