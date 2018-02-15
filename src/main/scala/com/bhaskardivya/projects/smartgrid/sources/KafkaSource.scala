package com.bhaskardivya.projects.smartgrid.sources

import java.util.Properties

import com.bhaskardivya.projects.smartgrid.model.SensorEvent
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.slf4j.LoggerFactory
import org.slf4j.Logger

/**
  * Create a consumer for kafka as a stream source
  */
class KafkaSource() {
  val LOG: Logger = LoggerFactory.getLogger("KafkaSource")

  def getSource(env: StreamExecutionEnvironment, params: ParameterTool): DataStream[SensorEvent] = {
    val topic = params.get("topic", "test")
    val server = params.get("bootstrap.servers", "localhost:9092")
    val groupId = params.get("group.id", "test-" + System.currentTimeMillis().toString)
    val startFromEarliest = params.getBoolean("start-from-earliest", true)

    // Create properties map for Kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", server)
    properties.setProperty("group.id", groupId)

    val consumer = new FlinkKafkaConsumer011[SensorEvent](topic, SensorEvent.schema(env.getConfig), properties)

    // Always start from the earliest kafka offset
    if(startFromEarliest)
      consumer.setStartFromEarliest()

    // Assign the Timestamp and Watermark from the SensorEvent's timestamp field
    consumer.assignTimestampsAndWatermarks(SensorEvent.tsAssigner())

    LOG.info("Created Source from kafka - Topic: {}, Server: {}, Consumer Group: {}", topic, server, groupId)

    env.addSource(consumer)
  }
}
