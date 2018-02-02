package com.bhaskardivya.projects.smartgrid.base

import java.io.File

import com.bhaskardivya.projects.smartgrid.model._
import com.bhaskardivya.projects.smartgrid.operators.{AverageAggregateWithKey, MedianAggregateWithKey, PredictionFunction}
import com.bhaskardivya.projects.smartgrid.pipeline._
import com.bhaskardivya.projects.smartgrid.sinks.{PredictionElasticSearchSink, SensorEventElasticSearchSink}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Abstract class that represents the Main Job that calculates the
  * averages and medians which are being used here itself to predict
  * the load forecast
  */
abstract class SensorEventAveragingJobBase extends Serializable {

  private val LOG_DIR  = "/data/" + getKeyName()

  /**
    * Method that returns the name of the key in the source datum
    *
    * @return String key name
    */
  def getKeyName(): String

  /**
    * Method to prepare the raw events properly aggregated(sum) based on the key
    * @param dataStream
    * @return
    */
  def initializeFlow(dataStream: DataStream[SensorEvent]): DataStream[SensorEvent]

  /**
    * Method that returns the value of the key in the source datum
    *
    * @param element
    * @return Long Key value
    */
  def getKey(element: SensorEvent): SensorKeyObject

  /**
    * Value of the HBase column family where the averages will be stored
    * @return String Name of the column family
    */
  def getTargetColumnFamily(): String

  object keyGetter extends AbstractKeyGetter {
    def apply(element: SensorEvent): SensorKeyObject = {
      getKey(element)
    }
  }

  def main(args: Array[String]): Unit = {
    //Create the log dirs
    try {
      new File(LOG_DIR).mkdir()
      new File(LOG_DIR + "/input/").mkdir()
      new File(LOG_DIR + "/output_avg/").mkdir()
      new File(LOG_DIR + "/output_enriched/").mkdir()
      new File(LOG_DIR + "/output_prediction/").mkdir()
    } catch {
      case e: Exception => println("Directories already created")
    }

    // parse parameters
    val params = ParameterTool.fromArgs(args)

    // Initialise the environment for flink
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // will be using the timestamp from the records
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Get the stream according to params
    val stream: DataStream[SensorEvent] = SourceChooser.from(env, params).name("Sensor Source")

    val withTimestamps = stream
      .assignTimestampsAndWatermarks(SensorEvent.tsAssigner())
      .name("Source with Timestamp")

    // Create a stream with sum according to the key specified
    val initializedFlow = initializeFlow(withTimestamps)

    // Streams for each window duration for the average
    val avg_windowed1min = initializedFlow
      .keyBy(keyGetter(_))
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new AverageAggregateWithKey(keyGetter))
      .name(getKeyName() + "Average for 1 min Window")

    val avg_windowed5min = avg_windowed1min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name(getKeyName() + "Average for 5 min Window")

    val avg_windowed15min = avg_windowed5min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name(getKeyName() + "Average for 15 min Window")

    val avg_windowed60min = avg_windowed15min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name(getKeyName() + "Average for 60 min Window")

    val avg_windowed120min = avg_windowed60min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(120), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .name(getKeyName() + "Average for 120 min Window")

    // Write to file for debug
    val debug = params.has("debug")
    if (debug) {
      initializedFlow.writeAsCsv(LOG_DIR + "/input/initializedFlow.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Initialized Flow")
      avg_windowed1min.writeAsCsv(LOG_DIR + "/output_avg/windowed1min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Avg windowed 1 Min")
      avg_windowed5min.writeAsCsv(LOG_DIR + "/output_avg/windowed5min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Avg windowed 5 Min")
      avg_windowed15min.writeAsCsv(LOG_DIR + "/output_avg/windowed15min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Avg windowed 15 Min")
      avg_windowed60min.writeAsCsv(LOG_DIR + "/output_avg/windowed60min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Avg windowed 60 Min")
      avg_windowed120min.writeAsCsv(LOG_DIR + "/output_avg/windowed120min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Avg windowed 120 Min")
    }

    val median_windowed1min = avg_windowed1min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new MedianAggregateWithKey)
      .name(getKeyName() + "Median for 1 min Window")

    val median_windowed5min = median_windowed1min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(MedianLoadWithKey.reducer)
      .name(getKeyName() + "Median for 5 min Window")

    val median_windowed15min = median_windowed5min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(MedianLoadWithKey.reducer)
      .name(getKeyName() + "Median for 15 min Window")

    val median_windowed60min = median_windowed15min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(MedianLoadWithKey.reducer)
      .name(getKeyName() + "Median for 60 min Window")

    val median_windowed120min = median_windowed60min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(120), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(MedianLoadWithKey.reducer)
      .name(getKeyName() + "Median for 120 min Window")

    val timeout = params.getLong("timeout", 3000000L) // 300 seconds timeout

    // Enrich the average calculation with the median value for each stream of different window duration
    val windowed1min_enriched: DataStream[(AverageWithKey, MedianLoadWithKey)] = avg_windowed1min
      .join(median_windowed1min)
      .where(_.key).equalTo(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(Constants.SLIDING_INTERVAL)))
      .apply((_, _))
      .startNewChain()
      .name(getKeyName() + " Enriched - 1 Min Window")

    val windowed5min_enriched: DataStream[(AverageWithKey, MedianLoadWithKey)] = avg_windowed5min
      .join(median_windowed5min)
      .where(_.key).equalTo(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(Constants.SLIDING_INTERVAL)))
      .apply((_, _))
      .startNewChain()
      .name(getKeyName() + " Enriched - 5 Min Window")

    val windowed15min_enriched: DataStream[(AverageWithKey, MedianLoadWithKey)] = avg_windowed15min
      .join(median_windowed15min)
      .where(_.key).equalTo(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.seconds(Constants.SLIDING_INTERVAL)))
      .apply((_, _))
      .startNewChain()
      .name(getKeyName() + " Enriched - 15 Min Window")

    val windowed60min_enriched: DataStream[(AverageWithKey, MedianLoadWithKey)] = avg_windowed60min
      .join(median_windowed60min)
      .where(_.key).equalTo(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.seconds(Constants.SLIDING_INTERVAL)))
      .apply((_, _))
      .startNewChain()
      .name(getKeyName() + " Enriched - 60 Min Window")

    val windowed120min_enriched: DataStream[(AverageWithKey, MedianLoadWithKey)] = avg_windowed120min
      .join(median_windowed120min)
      .where(_.key).equalTo(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(120), Time.seconds(Constants.SLIDING_INTERVAL)))
      .apply((_, _))
      .startNewChain()
      .name(getKeyName() + " Enriched - 120 Min Window")

    if (debug) {
      windowed1min_enriched.writeAsCsv(LOG_DIR + "/output_enriched/windowed1min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Enriched 1 Min Window")
      windowed5min_enriched.writeAsCsv(LOG_DIR + "/output_enriched/windowed5min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Enriched 5 Min Window")
      windowed15min_enriched.writeAsCsv(LOG_DIR + "/output_enriched/windowed15min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Enriched 15 Min Window")
      windowed60min_enriched.writeAsCsv(LOG_DIR + "/output_enriched/windowed60min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Enriched 60 Min Window")
      windowed120min_enriched.writeAsCsv(LOG_DIR + "/output_enriched/windowed120min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Enriched 120 Min Window")
    }

    // Create the predicted value streams
    val windowed1min_prediction = windowed1min_enriched
      .map(new PredictionFunction(entity = getKeyName(), slidingWindow = Time.minutes(1).toMilliseconds))

    val windowed5min_prediction = windowed5min_enriched
      .map(new PredictionFunction(entity = getKeyName(), slidingWindow = Time.minutes(5).toMilliseconds))

    val windowed15min_prediction = windowed15min_enriched
      .map(new PredictionFunction(entity = getKeyName(), slidingWindow = Time.minutes(15).toMilliseconds))

    val windowed60min_prediction = windowed60min_enriched
      .map(new PredictionFunction(entity = getKeyName(), slidingWindow = Time.minutes(60).toMilliseconds))

    val windowed120min_prediction = windowed120min_enriched
      .map(new PredictionFunction(entity = getKeyName(), slidingWindow = Time.minutes(120).toMilliseconds))

    // Sink the Predicted value streams to Elasticsearch
    windowed1min_prediction
      .addSink(PredictionElasticSearchSink(params,Constants.ES_INDEX_NAME, Constants.ES_INDEX_TYPE_1MIN))
      .name(getKeyName() + " Prediction Sink - ES - 1 min Window")

    windowed5min_prediction
      .addSink(PredictionElasticSearchSink(params,Constants.ES_INDEX_NAME, Constants.ES_INDEX_TYPE_5MIN))
      .name(getKeyName() + " Prediction Sink - ES - 5 min Window")

    windowed15min_prediction
      .addSink(PredictionElasticSearchSink(params,Constants.ES_INDEX_NAME, Constants.ES_INDEX_TYPE_15MIN))
      .name(getKeyName() + " Prediction Sink - ES - 15 min Window")

    windowed60min_prediction
      .addSink(PredictionElasticSearchSink(params,Constants.ES_INDEX_NAME, Constants.ES_INDEX_TYPE_60MIN))
      .name(getKeyName() + " Prediction Sink - ES - 60 min Window")

    windowed120min_prediction
      .addSink(PredictionElasticSearchSink(params,Constants.ES_INDEX_NAME, Constants.ES_INDEX_TYPE_120MIN))
      .name(getKeyName() + " Prediction Sink - ES - 120 min Window")

    if (debug) {
      windowed1min_prediction.writeAsCsv(LOG_DIR + "/output_prediction/windowed1min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed5min_prediction.writeAsCsv(LOG_DIR + "/output_prediction/windowed5min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed15min_prediction.writeAsCsv(LOG_DIR + "/output_prediction/windowed15min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed60min_prediction.writeAsCsv(LOG_DIR + "/output_prediction/windowed60min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed120min_prediction.writeAsCsv(LOG_DIR + "/output_prediction/windowed120min.csv", FileSystem.WriteMode.OVERWRITE)
    }

    //Sink the original feed into ES as well
    initializedFlow
      .addSink(SensorEventElasticSearchSink(params, Constants.ES_INDEX_NAME, Constants.ES_INDEX_TYPE_RAW))
      .name("Sensor Raw to ES")

    env.execute("Sensor Event" + getKeyName() + " Prediction Job (Kafka to HBase Averages + Prediction to ES)")

  }

}