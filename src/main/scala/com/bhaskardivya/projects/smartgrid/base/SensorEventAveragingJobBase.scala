package com.bhaskardivya.projects.smartgrid.base

import java.io.File

import com.bhaskardivya.projects.smartgrid.model._
import com.bhaskardivya.projects.smartgrid.operators._
import com.bhaskardivya.projects.smartgrid.pipeline._
import com.bhaskardivya.projects.smartgrid.sinks.PredictionElasticSearchSink
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
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
    val rawStream: DataStream[SensorEvent] = SourceChooser.from(env, params).name("Sensor Source with Timestamp")

    //TODO: create a Global Window for work values which will output the missing load values

    // Create a stream with sum according to the key specified
    val initializedFlow = initializeFlow(rawStream)

    implicit val typeInfoAverageWithKey = TypeInformation.of(classOf[AverageWithKey])

    // Streams for each window duration for the average
    val avg_windowed1min = initializedFlow
      .map(e => AverageWithKey(keyGetter(e), Slice(Time.minutes(1))(e.timestamp), Average(e.value, 1)))
      .keyBy(e => (e.key, e.slice.start_time_of_day))
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .reduce(new AverageWithKeyReducer)
      .name(getKeyName() + " Average for 1 min Tumbling Window")

    // Store median as operator state
    avg_windowed1min
      .keyBy(e => (e.key, e.slice.start_time_of_day))
      .flatMap(new MedianWithKeyMapper)
      .name(getKeyName() + " Median state for 1 min Tumbling Window")

    implicit val typeInfoPrediction2 = TypeInformation.of(classOf[Prediction2])

    val windowed1min_prediction = avg_windowed1min
      .keyBy(e => (e.key, e.slice.start_time_of_day))
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
      .reduce(new AverageWithKeyReducer)
      .keyBy(e => (e.key, e.slice.start_time_of_day))
      .flatMap(new EnrichMapper)
      .name(getKeyName() + " Prediction values for 1 min")

    // Sink the Predicted value streams to Elasticsearch
    windowed1min_prediction
      .addSink(PredictionElasticSearchSink(params,Constants.ES_INDEX_NAME, Constants.ES_INDEX_TYPE_1MIN))
      .name(getKeyName() + " Prediction Sink - ES - 1 min Window")

    // Write to file for debug
    val debug = params.has("debug")
    if (debug) {
      avg_windowed1min.writeAsText(LOG_DIR + "/output_avg/windowed1min.csv", FileSystem.WriteMode.OVERWRITE).name("Debug Avg 1 Min Window")
      windowed1min_prediction.writeAsText(LOG_DIR + "/output_prediction/windowed1min.csv", FileSystem.WriteMode.OVERWRITE)
      /*initializedFlow
          .addSink(SensorEventElasticSearchSink(params, Constants.ES_INDEX_NAME, Constants.ES_INDEX_TYPE_RAW))
          .name("Sensor Raw to ES")*/
    }

    env.execute("Sensor Event" + getKeyName() + " Prediction Job")
  }
}