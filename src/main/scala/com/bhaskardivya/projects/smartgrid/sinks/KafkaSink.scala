package com.bhaskardivya.projects.smartgrid.sinks

import com.bhaskardivya.projects.smartgrid.model.SensorEvent
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

class KafkaSink {

  def apply(env: StreamExecutionEnvironment,brokerList: String, topicId: String): FlinkKafkaProducer011[SensorEvent] = {
    new FlinkKafkaProducer011[SensorEvent] (brokerList, topicId, SensorEvent.schema(env.getConfig))
  }

  def apply: KafkaSink = new KafkaSink()
}
