package com.bhaskardivya.projects.smartgrid.job

import com.bhaskardivya.projects.smartgrid.base.SensorEventAveragingJobBase
import com.bhaskardivya.projects.smartgrid.model.{Constants, SensorEvent, SensorKeyObject}
import org.apache.flink.streaming.api.scala.DataStream

object PlugAveragingJob extends SensorEventAveragingJobBase with Serializable{

  override def getKeyName(): String = "Plug"

  override def getKey(element: SensorEvent): SensorKeyObject = SensorKeyObject(element.house_id, element.household_id, element.plug_id)

  override def getTargetColumnFamily(): String = Constants.PLUG_CF

  override def initializeFlow(dataStream: DataStream[SensorEvent]) = {
    // Sum all the multiple values with the same timestamp for a given plug of a given house
    dataStream
      .keyBy("house_id", "plug_id", "timestamp")
      .sum("value")
      .name("Aggregated by Plug Data")
  }
}
