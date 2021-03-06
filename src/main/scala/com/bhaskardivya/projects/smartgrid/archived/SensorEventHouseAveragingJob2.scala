package com.bhaskardivya.projects.smartgrid.archived

import com.bhaskardivya.projects.smartgrid.model.{Constants, SensorEvent}
import com.bhaskardivya.projects.smartgrid.operators.AverageAggregate
import com.bhaskardivya.projects.smartgrid.pipeline._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SensorEventHouseAveragingJob2 {
  def main(args: Array[String]): Unit = {
    // parse parameters
    val params = ParameterTool.fromArgs(args)

    // Initialise the environment for flink
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // will be using the timestamp from the records
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Get the stream according to params
    val stream: DataStream[SensorEvent] = SourceChooser.from(env, params).name("Sensor Raw")

    val withTimestampsKeyed = stream
      //id, timestamp, value, property, plug_id, household_id, house_id
      //.map(x => SensorEvent(x(0).toLong, x(1).toLong, x(2).toDouble, x(3).toInt, x(4).toLong, x(5).toLong, x(6).toLong))
      .assignTimestampsAndWatermarks(SensorEvent.tsAssigner())
      .name("Kafka Source with TS")
      .keyBy(_.house_id)

    val windowed1min = withTimestampsKeyed
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new AverageAggregate())//, new AverageWindowFunction())
      .name("Average for 1 min Window")

    windowed1min.writeAsCsv("/data/window_1min_out.csv", FileSystem.WriteMode.OVERWRITE)

    val windowed5min = withTimestampsKeyed
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new AverageAggregate())//, new AverageWindowFunction())
      .name("Average for 5 min Window")

    val windowed15min = withTimestampsKeyed
      .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new AverageAggregate())//, new AverageWindowFunction())
      .name("Average for 15 min Window")

    val windowed60min = withTimestampsKeyed
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new AverageAggregate())//, new AverageWindowFunction())
      .name("Average for 60 min Window")

    val windowed120min = withTimestampsKeyed
      .window(SlidingEventTimeWindows.of(Time.minutes(120), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new AverageAggregate())//, new AverageWindowFunction())
      .name("Average for 120 min Window")

   /* windowed1min.addSink(new OutputFormatSinkFunction[(Long, Average)](new HBaseOutputFormat().of(Constants.TABLE_1MIN, Constants.HOUSE_CF)))
      .name("House - 1 Min Window")

    windowed5min.addSink(new OutputFormatSinkFunction[(Long, Average)](new HBaseOutputFormat().of(Constants.TABLE_5MIN, Constants.HOUSE_CF)))
      .name("House - 5 Min Window")

    windowed15min.addSink(new OutputFormatSinkFunction[(Long, Average)](new HBaseOutputFormat().of(Constants.TABLE_15MIN, Constants.HOUSE_CF)))
      .name("House - 15 Min Window")

    windowed60min.addSink(new OutputFormatSinkFunction[(Long, Average)](new HBaseOutputFormat().of(Constants.TABLE_60MIN, Constants.HOUSE_CF)))
      .name("House - 60 Min Window")

    windowed120min.addSink(new OutputFormatSinkFunction[(Long, Average)](new HBaseOutputFormat().of(Constants.TABLE_120MIN, Constants.HOUSE_CF)))
      .name("House - 120 Min Window")*/

    env.execute("Sensor Event House Averaging Job (Kafka to HBase Averages) ")

  }
}