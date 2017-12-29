package com.bhaskardivya.projects.smartgrid.archived

import com.bhaskardivya.projects.smartgrid.base.AbstractKeyGetter
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

object SensorEventHouseAveragingJob {
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

    val windowed1min = withTimestamps
      .keyBy(keyGetter(_))
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new AverageAggregateWithKey(keyGetter))
      .name("Average for 1 min Window")

    val windowed5min = windowed1min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name("Average for 5 min Window")

    val windowed15min = windowed5min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name("Average for 15 min Window")

    val windowed60min = windowed15min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name("Average for 60 min Window")

    val windowed120min = windowed60min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(120), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name("Average for 120 min Window")

    val debug = params.has("debug")
    if(debug){
      windowed1min.writeAsCsv("/data/output.windowed1min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed5min.writeAsCsv("/data/output.windowed5min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed15min.writeAsCsv("/data/output.windowed15min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed60min.writeAsCsv("/data/output.windowed60min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed120min.writeAsCsv("/data/output.windowed120min.csv", FileSystem.WriteMode.OVERWRITE)
    }

    windowed1min.addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_1MIN, Constants.HOUSE_CF)))
      .name("House - 1 Min Window")

    windowed5min.addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_5MIN, Constants.HOUSE_CF)))
      .name("House - 5 Min Window")

    windowed15min.addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_15MIN, Constants.HOUSE_CF)))
      .name("House - 15 Min Window")

    windowed60min.addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_60MIN, Constants.HOUSE_CF)))
      .name("House - 60 Min Window")

    windowed120min.addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_120MIN, Constants.HOUSE_CF)))
      .name("House - 120 Min Window")

    env.execute("Sensor Event House Averaging Job (Kafka to HBase Averages) ")

  }

  object keyGetter extends AbstractKeyGetter{
    def apply(element: SensorEvent): String = {
      element.house_id.toString
    }
  }

}