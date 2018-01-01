package com.bhaskardivya.projects.smartgrid.model

import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.sling.commons.json.JSONObject



/**
  * id, timestamp, value, property, plug_id, household_id, house_id
  */
case class SensorEvent(id: Long, timestamp: Long, value: Double, property: Int, plug_id: Long, household_id: Long, house_id: Long){
  override def toString: String = ToStringBuilder.reflectionToString(this)
  def getTimeMillis(): Long = this.timestamp*1000

  def toJSONString():String = {
    toJSON().toString()
  }

  def toJSON(): JSONObject = {
    val json: JSONObject = new JSONObject()
    json.put("id", this.id)
    json.put("timestamp", this.timestamp)
    json.put("value", this.value)
    json.put("property", this.property)
    json.put("plug_id", this.plug_id)
    json.put("household_id", this.household_id)
    json.put("house_id", this.house_id)

    json
  }
}

object SensorEvent {

  def fromString(line: String): SensorEvent = {
    val tokens: Array[String] = line.split(",")

    try {
      val id: Long = tokens(0).toLong
      val timestamp: Long = tokens(1).toLong
      val value: Double = tokens(2).toDouble
      val property: Int = tokens(3).toInt
      val plug_id: Long = tokens(4).toLong
      val household_id: Long = tokens(5).toLong
      val house_id: Long = tokens(6).toLong

      new SensorEvent(id, timestamp, value, property, plug_id, household_id, house_id)
    } catch {
      case e: Exception => null
    }
  }

  def schema(implicit config: ExecutionConfig): TypeInformationSerializationSchema[SensorEvent] = {
    val info = TypeInformation.of(new TypeHint[SensorEvent](){})
    new TypeInformationSerializationSchema[SensorEvent](info, config)
  }

  val MAX_DELAY: Time = Time.seconds(10)

  def tsAssigner(): BoundedOutOfOrdernessTimestampExtractor[SensorEvent] = {
    new BoundedOutOfOrdernessTimestampExtractor[SensorEvent](MAX_DELAY) {
      override def extractTimestamp(element: SensorEvent) = element.getTimeMillis()
    }
  }
}