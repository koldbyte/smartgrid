package com.bhaskardivya.projects.smartgrid.base

import com.bhaskardivya.projects.smartgrid.model._
import com.bhaskardivya.projects.smartgrid.operators.AverageAggregateWithKey
import com.bhaskardivya.projects.smartgrid.pipeline._
import com.bhaskardivya.projects.smartgrid.sinks.HBaseOutputFormatAverageWithKey
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

abstract class SensorEventAveragingJobBase extends Serializable {

  /**
    * Method that returns the name of the key in the source datum
    *
    * @return String key name
    */
  def getKeyName(): String

  def initializeFlow(dataStream: DataStream[SensorEvent]): DataStream[SensorEvent]

  /**
    * Method that returns the value of the key in the source datum
    *
    * @param element
    * @return Long Key value
    */
  def getKey(element: SensorEvent): String

  /**
    * Value of the HBase column family where the averages will be stored
    * @return String Name of the column family
    */
  def getTargetColumnFamily(): String

  object keyGetter extends AbstractKeyGetter {
    def apply(element: SensorEvent): String = {
      getKey(element)
    }
  }

  def main(args: Array[String]): Unit = {
    // parse parameters
    val params = ParameterTool.fromArgs(args)

    // Initialise the environment for flink
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // will be using the timestamp from the records
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Get the stream according to params
    val stream: DataStream[SensorEvent] = SourceChooser.from(env, params).name("Sensor Parsed Data")

    val withTimestamps = stream
      .assignTimestampsAndWatermarks(SensorEvent.tsAssigner())
      .name("Kafka Source with TS")

    val initializedFlow = initializeFlow(withTimestamps)

    val windowed1min = initializedFlow
      .keyBy(keyGetter(_))
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new AverageAggregateWithKey(keyGetter))
      .name(getKeyName() + "Average for 1 min Window")

    val windowed5min = windowed1min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name(getKeyName() + "Average for 5 min Window")

    val windowed15min = windowed5min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name(getKeyName() + "Average for 15 min Window")

    val windowed60min = windowed15min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name(getKeyName() + "Average for 60 min Window")

    val windowed120min = windowed60min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(120), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name(getKeyName() + "Average for 120 min Window")

    val debug = params.has("debug")
    if (debug) {
      initializedFlow.writeAsCsv("/data/output.initializedFlow.csv", FileSystem.WriteMode.OVERWRITE)
      windowed1min.writeAsCsv("/data/output.windowed1min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed5min.writeAsCsv("/data/output.windowed5min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed15min.writeAsCsv("/data/output.windowed15min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed60min.writeAsCsv("/data/output.windowed60min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed120min.writeAsCsv("/data/output.windowed120min.csv", FileSystem.WriteMode.OVERWRITE)
    }

    windowed1min.addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_1MIN, getTargetColumnFamily())))
      .name(getKeyName() + " HBase - 1 Min Window")

    windowed5min.addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_5MIN, getTargetColumnFamily())))
      .name(getKeyName() + " HBase - 5 Min Window")

    windowed15min.addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_15MIN, getTargetColumnFamily())))
      .name(getKeyName() + " HBase - 15 Min Window")

    windowed60min.addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_60MIN, getTargetColumnFamily())))
      .name(getKeyName() + " HBase - 60 Min Window")

    windowed120min.addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_120MIN, getTargetColumnFamily())))
      .name(getKeyName() + " HBase - 120 Min Window")

    env.execute("Sensor Event" + getKeyName() + " Averaging Job (Kafka to HBase Averages)")

  }

}