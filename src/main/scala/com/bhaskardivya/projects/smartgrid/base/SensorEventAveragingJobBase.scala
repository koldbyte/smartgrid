package com.bhaskardivya.projects.smartgrid.base

import java.io.File

import com.bhaskardivya.projects.smartgrid.model._
import com.bhaskardivya.projects.smartgrid.operators._
import com.bhaskardivya.projects.smartgrid.pipeline._
import com.bhaskardivya.projects.smartgrid.sinks.ElasticSearchSink
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Abstract class that represents the Main Job that calculates the
  * averages and medians which are being used here itself to predict
  * the load forecast
  */
abstract class SensorEventAveragingJobBase extends Serializable {

  //register implicits for types
  implicit val typeInfoAverageWithKey: TypeInformation[AverageWithKey] = TypeInformation.of(classOf[AverageWithKey])
  implicit val typeInfoPrediction2: TypeInformation[Prediction] = TypeInformation.of(classOf[Prediction])

  private val LOG_DIR  = "/data/" + getKeyName()

  /**
    * Method that returns the name of the key in the source datum
    *
    * @return String key name
    */
  def getKeyName(): String

  /**
    * Method to prepare the raw events properly aggregated(sum) based on the key
    * @param dataStream source raw data stream
    * @return
    */
  def initializeFlow(dataStream: DataStream[SensorEvent]): DataStream[SensorEvent]

  /**
    * Method that returns the value of the key in the source datum
    *
    * @param element  SensorEvent record
    * @return Long    Key value
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

  // Sliding window durations in minutes
  val windowDurations = List(1, 5, 15, 60, 120)

  def main(args: Array[String]): Unit = {

    //Create the log dirs
    try {
      new File(LOG_DIR).mkdir()
      new File(LOG_DIR + "/state/").mkdir()
      new File(LOG_DIR + "/input/").mkdir()
      new File(LOG_DIR + "/output_avg/").mkdir()
      new File(LOG_DIR + "/output_prediction/").mkdir()
    } catch {
      case e: Exception => println("Directories already created" + e.getMessage)
    }

    // parse parameters
    val params = ParameterTool.fromArgs(args)

    // Initialise the environment for flink
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // will be using the timestamp from the records
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //env.enableCheckpointing(10000) // checkpoint every 10000 msecs
    //env.setStateBackend(new FsStateBackend(LOG_DIR +"/state/"))

    // Get the stream according to params
    val rawStream: DataStream[SensorEvent] = SourceChooser.from(env, params).name("Sensor Source with Timestamp")

    // Create a Global Window for work values which will output the missing load values
    val averageUsingWorkValues: DataStream[AverageWithKey] = rawStream
      .filter(_.property == Constants.PROPERTY_WORK)
      .keyBy(e => getKey(e))
      .countWindow(2,1)
      .process(new WorkValueProcessWindow)
      .flatMap(new WorkValueFlatMap(Time.minutes(1))(keyGetter))

    // Create a stream with sum according to the key specified
    val initializedFlow = initializeFlow(rawStream)
    val averageWithKeys = initializedFlow
        .map(e => AverageWithKey(keyGetter(e), Slice(Time.minutes(1))(e.timestamp), Average(e.value, 1)))
        // Assumption: If both the Load values and work Values are available in a slice,
        // Average of them will still be closer to the real measurement
        .union(averageUsingWorkValues)

    //create median states for various window duration
    createMedianState(averageWithKeys)

    // Create the average sliding window stream and corresponding Prediction Stream
    val windowed_average_1min = createAverageStream(params, 1, averageWithKeys)
    createPredictionStream(params, 1, windowed_average_1min)

    val windowed_average_5min = createAverageStream(params, 5, windowed_average_1min)
    createPredictionStream(params, 5, windowed_average_5min)

    val windowed_average_15min = createAverageStream(params, 15, windowed_average_5min)
    createPredictionStream(params, 15, windowed_average_15min)

    val windowed_average_60min = createAverageStream(params, 60, windowed_average_15min)
    createPredictionStream(params, 60, windowed_average_60min)

    val windowed_average_120min = createAverageStream(params, 120, windowed_average_60min)
    createPredictionStream(params, 120, windowed_average_120min)

    initializedFlow
        .addSink(ElasticSearchSink[SensorEvent](params, Constants.ES_INDEX_NAME, Constants.ES_INDEX_TYPE_RAW))
        .name("Sensor Raw to ES")

    env.execute("Sensor Event" + getKeyName() + " Prediction Job")
  }

  /**
    * Helper function to create median MapState for each window duration
    * @param averageWithKeyStream   DataStream of AverageWithKey
    */
  def createMedianState(averageWithKeyStream: DataStream[AverageWithKey]) = {

    // Streams for each window duration for the average
    windowDurations.foreach(duration => {

      val stateName = getStateName(duration)

      val avg_windowed = averageWithKeyStream
        .keyBy(_.key)
        .window(TumblingEventTimeWindows.of(Time.minutes(duration)))
        .reduce(new AverageWithKeyReducer)
        .name(getKeyName() + " Average for " + duration + " min Tumbling Window")

      // Store median as operator state
      avg_windowed
        .keyBy(_.key)
        .flatMap(new MedianWithKeyMapper(stateName))
        .name(getKeyName() + " Median state for " + duration + " min Tumbling Window")

    })
  }

  def createAverageStream(params: ParameterTool, duration: Int, sourceStream: DataStream[AverageWithKey]) = {

    val windowed_average = sourceStream
      // Map the Correct Slice duration
      .map(e => AverageWithKey(e.key, Slice(Time.minutes(duration))(e.slice.timestamp), e.average))
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(duration), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(new AverageWithKeyReducer)

    windowed_average
  }

  def createPredictionStream(params: ParameterTool, duration: Int, averageStream: DataStream[AverageWithKey]) = {
    val windowed_prediction = averageStream
      .keyBy(_.key)
      .flatMap(new EnrichMapper(getStateName(duration)))
      .name(getKeyName() + " Prediction values for " + duration + " min")

    // Sink the Predicted value streams to Elasticsearch
    windowed_prediction
      .addSink(ElasticSearchSink[Prediction](params, Constants.ES_INDEX_NAME, Constants.ES_INDEX_TYPE_1MIN))
      .name(getKeyName() + " Prediction Sink - ES - " + duration + "  min Window")

    // Write to file for debug
    if(params.has("debug")){
      windowed_prediction.writeAsText(LOG_DIR + "/output_prediction/windowed"+ duration +"min.csv", FileSystem.WriteMode.OVERWRITE)
    }

    windowed_prediction
  }

  def getStateName(duration: Int) = "median-" + duration + "min"
}