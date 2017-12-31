package com.bhaskardivya.projects.smartgrid.base

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import com.bhaskardivya.projects.smartgrid.model._
import com.bhaskardivya.projects.smartgrid.operators.{AverageAggregateWithKey, HBaseAsyncFunction, PredictionFunction}
import com.bhaskardivya.projects.smartgrid.pipeline._
import com.bhaskardivya.projects.smartgrid.sinks.{HBaseOutputFormatAverageWithKey, PredictionElasticSearchSink}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink

abstract class SensorEventAveragingJobBase extends Serializable {

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

    // Create a stream with sum according to the key specified
    val initializedFlow = initializeFlow(withTimestamps)

    // Streams for each window duration for the average
    val windowed1min = initializedFlow
      .keyBy(keyGetter(_))
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(Constants.SLIDING_INTERVAL)))
      .aggregate(new AverageAggregateWithKey(keyGetter))
      .startNewChain()
      .name(getKeyName() + "Average for 1 min Window")

    val windowed5min = windowed1min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .startNewChain()
      .name(getKeyName() + "Average for 5 min Window")

    val windowed15min = windowed5min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .startNewChain()
      .name(getKeyName() + "Average for 15 min Window")

    val windowed60min = windowed15min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .startNewChain()
      .name(getKeyName() + "Average for 60 min Window")

    val windowed120min = windowed60min
      .keyBy(_.key)
      .window(SlidingEventTimeWindows.of(Time.minutes(120), Time.seconds(Constants.SLIDING_INTERVAL)))
      .reduce(AverageWithKey.reducer)
      .startNewChain()
      .name(getKeyName() + "Average for 120 min Window")

    // Write to file for debug
    val debug = params.has("debug")
    if (debug) {
      initializedFlow.writeAsCsv("/data/output.initializedFlow.csv", FileSystem.WriteMode.OVERWRITE)
      windowed1min.writeAsCsv("/data/output.windowed1min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed5min.writeAsCsv("/data/output.windowed5min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed15min.writeAsCsv("/data/output.windowed15min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed60min.writeAsCsv("/data/output.windowed60min.csv", FileSystem.WriteMode.OVERWRITE)
      windowed120min.writeAsCsv("/data/output.windowed120min.csv", FileSystem.WriteMode.OVERWRITE)
    }

    // Write to HBase for each window duration
    windowed1min.disableChaining().addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_1MIN, getTargetColumnFamily())))
      .name(getKeyName() + " HBase - 1 Min Window")

    windowed5min.disableChaining().addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_5MIN, getTargetColumnFamily())))
      .name(getKeyName() + " HBase - 5 Min Window")

    windowed15min.disableChaining().addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_15MIN, getTargetColumnFamily())))
      .name(getKeyName() + " HBase - 15 Min Window")

    windowed60min.disableChaining().addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_60MIN, getTargetColumnFamily())))
      .name(getKeyName() + " HBase - 60 Min Window")

    windowed120min.disableChaining().addSink(new OutputFormatSinkFunction[AverageWithKey](new HBaseOutputFormatAverageWithKey().of(Constants.TABLE_120MIN, getTargetColumnFamily())))
      .name(getKeyName() + " HBase - 120 Min Window")

    val timeout = params.getLong("timeout", 1000000L)

    // Enrich the average calculation with the median value for each stream of different window duration
    val windowed1min_enriched: DataStream[(AverageWithKey, MedianLoad)] = AsyncDataStream.orderedWait(
      windowed1min,
      new HBaseAsyncFunction(Constants.TABLE_1MIN, getTargetColumnFamily()),
      timeout,
      TimeUnit.MILLISECONDS,
      1
    )
    .name(getKeyName() + " Enriched - 1 Min Window")

    val windowed5min_enriched: DataStream[(AverageWithKey, MedianLoad)] = AsyncDataStream.orderedWait(
      windowed5min,
      new HBaseAsyncFunction(Constants.TABLE_5MIN, getTargetColumnFamily()),
      timeout,
      TimeUnit.MILLISECONDS,
      1
    )
    .name(getKeyName() + " Enriched - 5 Min Window")

    val windowed15min_enriched: DataStream[(AverageWithKey, MedianLoad)] = AsyncDataStream.orderedWait(
      windowed15min,
      new HBaseAsyncFunction(Constants.TABLE_15MIN, getTargetColumnFamily()),
      timeout,
      TimeUnit.MILLISECONDS,
      1
    )
    .name(getKeyName() + " Enriched - 15 Min Window")

    val windowed60min_enriched: DataStream[(AverageWithKey, MedianLoad)] = AsyncDataStream.orderedWait(
      windowed60min,
      new HBaseAsyncFunction(Constants.TABLE_60MIN, getTargetColumnFamily()),
      timeout,
      TimeUnit.MILLISECONDS,
      1
    )
    .name(getKeyName() + " Enriched - 60 Min Window")

    val windowed120min_enriched: DataStream[(AverageWithKey, MedianLoad)] = AsyncDataStream.orderedWait(
      windowed120min,
      new HBaseAsyncFunction(Constants.TABLE_120MIN, getTargetColumnFamily()),
      timeout,
      TimeUnit.MILLISECONDS,
      1
    )
    .name(getKeyName() + " Enriched - 120 Min Window")

    if (debug) {
      windowed1min_enriched.writeAsCsv("/data/output.windowed1min_enriched.csv", FileSystem.WriteMode.OVERWRITE)
      windowed5min_enriched.writeAsCsv("/data/output.windowed5min_enriched.csv", FileSystem.WriteMode.OVERWRITE)
      windowed15min_enriched.writeAsCsv("/data/output.windowed15min_enriched.csv", FileSystem.WriteMode.OVERWRITE)
      windowed60min_enriched.writeAsCsv("/data/output.windowed60min_enriched.csv", FileSystem.WriteMode.OVERWRITE)
      windowed120min_enriched.writeAsCsv("/data/output.windowed120min_enriched.csv", FileSystem.WriteMode.OVERWRITE)
    }

    //Initialize Elastic search configuration
    val esClusterLocationIP = params.get("es.cluster.ip", "192.168.99.100")
    val esClusterLocationPort = params.getInt("es.cluster.port", 9300)
    val config = new java.util.HashMap[String, String]
    config.put("cluster.name", params.get("es.cluster.name", "docker-cluster"))
    // This instructs the sink to emit after every element, otherwise they would be buffered
    config.put("bulk.flush.max.actions", "1")

    val transportAddresses = new java.util.ArrayList[InetSocketAddress]
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName(esClusterLocationIP), esClusterLocationPort))

    // Create the predicted value streams
    val windowed1min_prediction = windowed1min_enriched
      .map(new PredictionFunction(getKeyName(), Time.minutes(1)))
      .addSink(new ElasticsearchSink[Prediction](config, transportAddresses, PredictionElasticSearchSink.of("", "")))
      .name(getKeyName() + " Prediction Sink - ES - 1 min Window")

    val windowed5min_prediction = windowed1min_enriched.map(new PredictionFunction(getKeyName(), Time.minutes(5)))
    val windowed15min_prediction = windowed1min_enriched.map(new PredictionFunction(getKeyName(), Time.minutes(15)))
    val windowed60min_prediction = windowed1min_enriched.map(new PredictionFunction(getKeyName(), Time.minutes(60)))
    val windowed120min_prediction = windowed1min_enriched.map(new PredictionFunction(getKeyName(), Time.minutes(120)))

    env.execute("Sensor Event" + getKeyName() + " Averaging Job (Kafka to HBase Averages)")

  }

}