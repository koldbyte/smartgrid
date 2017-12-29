/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bhaskardivya.projects.smartgrid.sources

import java.io._
import java.util.{Calendar, Random}
import java.util.zip.GZIPInputStream

import com.bhaskardivya.projects.smartgrid.model.SensorEvent
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark

import scala.collection.mutable

/**
 * This SourceFunction generates a data stream of plug records which are
 * read from a gzipped input file. Each record has a time stamp and the input file must be
 * ordered by this time stamp.
 *
 * In order to simulate a realistic stream source, the SourceFunction serves events proportional to
 * their timestamps. In addition, the serving of events can be delayed by a bounded random delay
 * which causes the events to be served out-of-order of their timestamps.
 *
 * The serving speed of the SourceFunction can be adjusted by a serving speed factor.
 * A factor of 60.0 increases the logical serving time by a factor of 60, i.e., events of one
 * minute (60 seconds) are served in 1 second.
 *
 * This SourceFunction is an EventSourceFunction and does continuously emit watermarks.
 * Hence it can only operate in event time mode which is configured as follows:
 *
 *   StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 *
 * @param dataFilePath The path to the gzipped input file.
 * @param maxDelaySecs The maximum serving delay. Defines how much elements are served out-of-order.
 * @param servingSpeed The relative serving speed. Can be used to fast-forward the stream.
 */
class CSVFileSource(dataFilePath: String, maxDelaySecs: Int, servingSpeed: Float)
  extends SourceFunction[SensorEvent] {

  private val maxDelayMsecs = maxDelaySecs * 1000
  private val watermarkDelayMSecs = if (maxDelayMsecs < 10000) 10000 else maxDelayMsecs

  @transient
  private var reader: BufferedReader = _
  @transient
  private var gzipStream: InputStream = _

  override def run(sourceContext: SourceContext[SensorEvent]): Unit = {
    gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath))
    reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"))

    println("BD | CSVFileSource | Stream created")

    if (this.maxDelayMsecs == 0) {
      generateOrderedStream(sourceContext)
    }
    else {
      generateUnorderedStream(sourceContext)
    }

    this.reader.close()
    this.reader = null
    this.gzipStream.close()
    this.gzipStream = null

    println("BD | CSVFileSource | Stream closed")
  }

  @throws(classOf[IOException])
  override def cancel(): Unit = {
    try {
      if (this.reader != null) {
        this.reader.close()
      }
      if (this.gzipStream != null) {
        this.gzipStream.close()
      }
    } finally {
      this.reader = null
      this.gzipStream = null
    }
  }

  @throws(classOf[Exception])
  private def generateOrderedStream(sourceContext: SourceContext[SensorEvent]) {
    println("BD | CSVFileSource | Generating ordered Stream")
    val servingStartTime = Calendar.getInstance.getTimeInMillis
    var dataStartTime = 0L
    var nextWatermark = 0L
    var nextWatermarkServingTime = 0L

    // read the first sensor event
    if (reader.ready) {
      //println("BD | CSVFileSource | First Reader ready")
      val line = reader.readLine
      if (line != null) {
        println("BD | CSVFileSource | First Line not null")
        val event = SensorEvent.fromString(line)

        // set time of first event
        dataStartTime = event.getTimeMillis()
        // initialize watermarks
        nextWatermark = dataStartTime + watermarkDelayMSecs
        nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)
        // emit first event
        sourceContext.collectWithTimestamp(event, event.getTimeMillis())

        println("BD | CSVFileSource | First Emitted to stream | event.timestamp")
      }
    }
    else {
      return
    }

    // read all following sensor events
    while (reader.ready) {
      //println("BD | CSVFileSource | All | Reader ready")
      val line = reader.readLine
      if (line != null) {
        //println("BD | CSVFileSource | All | Line not null")
        // read event
        val event = SensorEvent.fromString(line)

        val eventTime = event.getTimeMillis()
        val now = Calendar.getInstance.getTimeInMillis
        val eventServingTime = toServingTime(servingStartTime, dataStartTime, eventTime)

        // get time to wait until event and next watermark needs to be emitted
        val eventWait = eventServingTime - now
        val watermarkWait = nextWatermarkServingTime - now

        if (eventWait < watermarkWait) {
          // wait to emit next event
          Thread.sleep(if (eventWait > 0) eventWait else 0)
        }
        else if (eventWait > watermarkWait) {
          // wait to emit watermark
          Thread.sleep(if (watermarkWait > 0) watermarkWait else 0)
          // emit watermark
          sourceContext.emitWatermark(new Watermark(nextWatermark))
          // schedule next watermark
          nextWatermark = nextWatermark + watermarkDelayMSecs
          nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)
          // wait to emit event
          val remainWait: Long = eventWait - watermarkWait
          Thread.sleep(if (remainWait > 0) remainWait else 0)
        }
        else if (eventWait == watermarkWait) {
          // wait to emit watermark
          Thread.sleep(if (watermarkWait > 0) watermarkWait else 0)
          // emit watermark
          sourceContext.emitWatermark(new Watermark(nextWatermark - 1))
          // schedule next watermark
          nextWatermark = nextWatermark + watermarkDelayMSecs
          nextWatermarkServingTime = toServingTime(servingStartTime, dataStartTime, nextWatermark)
        }
        // emit event
        sourceContext.collectWithTimestamp(event, event.getTimeMillis())

        //println("BD | CSVFileSource | All | Emitted to stream " + SensorEvent.toString)
      }
    }
  }

  @throws(classOf[Exception])
  private def generateUnorderedStream(sourceContext: SourceContext[SensorEvent]) {

    val servingStartTime = Calendar.getInstance.getTimeInMillis
    var dataStartTime = 0L
    val rand: Random = new Random(7452)

    val emitSchedule = mutable.PriorityQueue.empty[(Long, Either[SensorEvent, Watermark])](
      Ordering.by( (_: (Long, Either[SensorEvent, Watermark]))._1 ).reverse
    )

    var event: SensorEvent = null
    if (reader.ready) {

      val line = reader.readLine
      if (line != null) {

        event = SensorEvent.fromString(line)
        dataStartTime = event.getTimeMillis()

        // schedule first event
        val delayedEventTime: Long = dataStartTime + getNormalDelayMsecs(rand)
        emitSchedule += ( (delayedEventTime, Left(event)) )
        // schedule first watermark
        val watermarkTime = dataStartTime + watermarkDelayMSecs
        val nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1)
        emitSchedule += ( (watermarkTime, Right(nextWatermark)) )
      }
    }
    else {
      return
    }

    if (reader.ready) {
      val line = reader.readLine
      if (line != null) {
        event = SensorEvent.fromString(line)
      }
    }

    while (emitSchedule.nonEmpty || reader.ready) {

      // insert all events into schedule that might be emitted next
      val curNextDelayedEventTime = if (emitSchedule.nonEmpty) emitSchedule.head._1 else -1
      var sensorEventTime = if (event != null) event.getTimeMillis() else -1
      while (event != null &&
        (emitSchedule.isEmpty || (sensorEventTime < curNextDelayedEventTime + maxDelayMsecs))) {

        // insert event into schedule
        val delayedEventTime = sensorEventTime + getNormalDelayMsecs(rand)
        emitSchedule += ( (delayedEventTime, Left(event)) )

        // read next sensor event from input
        if (reader.ready) {
          val line = reader.readLine
          if (line != null) {
            event = SensorEvent.fromString(line)
            sensorEventTime = event.getTimeMillis()
          } else {
            event = null
            sensorEventTime = -1
          }
        } else {
          event = null
          sensorEventTime = -1
        }
      }

      // emit next element
      val head = emitSchedule.dequeue()
      val delayedEventTime = head._1

      val now = Calendar.getInstance.getTimeInMillis
      val servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime)
      val waitTime = servingTime - now

      Thread.sleep(if (waitTime > 0) waitTime else 0)

      head._2 match {
        case Left(emitSensorEvent) => {
          // emit event
          sourceContext.collectWithTimestamp(emitSensorEvent, emitSensorEvent.getTimeMillis())
        }
        case Right(emitWatermark) => {
          // emit watermark
          sourceContext.emitWatermark(emitWatermark)
          // schedule next watermark
          val watermarkTime = delayedEventTime + watermarkDelayMSecs
          val nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1)
          emitSchedule += ( (watermarkTime, Right(nextWatermark)))
        }
      }
    }
  }

  def toServingTime(servingStartTime: Long, dataStartTime: Long, eventTime: Long): Long = {
    val dataDiff = eventTime - dataStartTime
    servingStartTime + (dataDiff / this.servingSpeed).toLong
  }

  def getNormalDelayMsecs(rand: Random): Long = {
    var delay = -1L
    val x = maxDelayMsecs / 2
    while (delay < 0 || delay > maxDelayMsecs) {
      delay = (rand.nextGaussian * x).toLong + x
    }
    delay
  }

}
