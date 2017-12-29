package com.bhaskardivya.projects.smartgrid.archived

import com.bhaskardivya.projects.smartgrid.model.{Average, Constants, SensorEvent}
import com.bhaskardivya.projects.smartgrid.operators.{AverageAggregate, AverageWindowFunction}
import com.bhaskardivya.projects.smartgrid.pipeline._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SensorEventPlugAveragingJob {
  def main(args: Array[String]): Unit = {
    // parse parameters
    val params = ParameterTool.fromArgs(args)

    // Initialise the environment for flink
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // will be using the timestamp from the records
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Get the stream according to params
    val stream: DataStream[SensorEvent] = SourceChooser.from(env, params).name("Sensor Raw")
    
    //no need to add timestamps as kafka source already has it
    //val withTimestamps = stream
      //id, timestamp, value, property, plug_id, household_id, house_id
      //.map(x => SensorEvent(x(0).toLong, x(1).toLong, x(2).toDouble, x(3).toInt, x(4).toLong, x(5).toLong, x(6).toLong))
      //.assignTimestampsAndWatermarks(new PunctuatedAssigner())

    val windowed1min = stream
      .keyBy(_.plug_id)
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new AverageAggregate(), new AverageWindowFunction())
      .name("Average for 1 min Window")

    val windowed5min = windowed1min
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(
        (a: (Long,Average), b: (Long, Average)) => (a._1, a._2+b._2)
      )
      .name("Average for 5 min Window")

    val windowed15min = windowed5min
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(
        (a: (Long,Average), b: (Long, Average)) => (a._1, a._2+b._2)
      )
      .name("Average for 15 min Window")

    val windowed60min = windowed15min
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(
        (a: (Long,Average), b: (Long, Average)) => (a._1, a._2+b._2)
      )
      .name("Average for 60 min Window")

    val windowed120min = windowed60min
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.minutes(120), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(
        (a: (Long,Average), b: (Long, Average)) => (a._1, a._2+b._2)
      )
      .name("Average for 120 min Window")
/*

    windowed1min.writeUsingOutputFormat(new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_1MIN, Constants.HOUSE_CF))
      .name("Plug - 1 Min Window")

    windowed5min.writeUsingOutputFormat(new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_5MIN, Constants.HOUSE_CF))
      .name("Plug - 5 Min Window")

    windowed15min.writeUsingOutputFormat(new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_15MIN, Constants.HOUSE_CF))
      .name("Plug - 15 Min Window")

    windowed60min.writeUsingOutputFormat(new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_60MIN, Constants.HOUSE_CF))
      .name("Plug - 60 Min Window")

    windowed120min.writeUsingOutputFormat(new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_120MIN, Constants.HOUSE_CF))
      .name("Plug - 120 Min Window")
*/

    env.execute("Sensor Event Plug Averaging Job (Kafka to HBase Averages) ")

  }
}