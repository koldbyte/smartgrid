package com.bhaskardivya.projects.smartgrid.job

import com.bhaskardivya.projects.smartgrid.base.SensorEventAveragingJobBase
import com.bhaskardivya.projects.smartgrid.model.{Constants, SensorEvent}
import org.apache.flink.streaming.api.scala.DataStream

object HouseAveragingJob extends SensorEventAveragingJobBase with Serializable{

  override def getKeyName(): String = "House"

  override def getKey(element: SensorEvent): String = element.house_id.toString

  override def getTargetColumnFamily(): String = Constants.HOUSE_CF

  override def initializeFlow(dataStream: DataStream[SensorEvent]) = {
    // Sum the values of all the plugs in a house with the same time stamp
    dataStream
      .keyBy("house_id", "timestamp")
      .sum("value")
  }
}
