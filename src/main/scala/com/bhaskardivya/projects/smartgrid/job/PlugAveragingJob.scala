package com.bhaskardivya.projects.smartgrid.job

import com.bhaskardivya.projects.smartgrid.base.SensorEventAveragingJobBase
import com.bhaskardivya.projects.smartgrid.model.{Constants, SensorEvent, SensorKeyObject}
import org.apache.flink.streaming.api.scala.DataStream

object PlugAveragingJob extends SensorEventAveragingJobBase with Serializable{

  override def getKeyName(): String = "Plug"

  override def getKey(element: SensorEvent): SensorKeyObject = SensorKeyObject(element.house_id, element.household_id, element.plug_id)

  override def getTargetColumnFamily(): String = Constants.PLUG_CF

  override def initializeFlow(dataStream: DataStream[SensorEvent]): DataStream[SensorEvent] = {
    // De-duplicate values with the same timestamp for a given plug of a given house
    dataStream
      .filter(_.property == Constants.PROPERTY_LOAD)
      .keyBy("house_id", "household_id" , "plug_id", "timestamp")
      .reduce((_,b) => b)
      .name("De-duplicated Raw stream")
  }
}
