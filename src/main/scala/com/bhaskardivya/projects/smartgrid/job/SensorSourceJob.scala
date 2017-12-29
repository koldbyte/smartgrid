package com.bhaskardivya.projects.smartgrid.job

import org.apache.flink.api.scala._
import com.bhaskardivya.projects.smartgrid.model.SensorEvent
import com.bhaskardivya.projects.smartgrid.sources.CSVFileSource
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object SensorSourceJob {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    if (params.has("help")) {
      println(
        """USAGE:
          |SensorSourceJob
          |--help\t\t Show this help
          |--input <Compressedfile>\t\tURL to the file to read
          |--maxServingDelay <0>\t\tAdd a delay to create out-of-order events
          |--servingSpeedFactor <1f>\t\t Speed factor against event time
          |--kafkaBroker <localhost:9092>\t\t URI to kafka Broker
          |--kafkaTopic <sensor_raw>\t\t Topic Name for Kafka""".stripMargin)
    }

    // input parameters
    val data = params.get("input", "/data/data.gz")
    val maxServingDelay = params.getInt("maxServingDelay", 0)
    val servingSpeedFactor = params.getFloat("servingSpeedFactor", 1f)

    // Target parameters
    val kafkaBroker = params.get("kafkaBroker", "localhost:9092")
    val kafkaTopic = params.get("kafkaTopic", "sensor_raw")
    val debug = params.has("debug")

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Read from file
    val events = env.addSource(new CSVFileSource(data, maxServingDelay, servingSpeedFactor)).name("CSV GZ File")

    // Write the generated data to a csv file
    if (debug)
      events.writeAsCsv("/data/output.csv", FileSystem.WriteMode.OVERWRITE)

    //Write to Kafka
    events.addSink(new FlinkKafkaProducer011[SensorEvent](kafkaBroker, kafkaTopic, SensorEvent.schema(env.getConfig))).name("Kafka Topic Sensor_raw")

    env.execute("Sensor Event Input Job (CSV to kafka)")
  }
}
