package com.bhaskardivya.projects.smartgrid.model

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable

/**
  * Class to represent a slice index of a size (aka window size)
  *
  * @param size       Window size
  * @param timestamp  Event timestamp from the record in seconds
  */
case class Slice(size: Time)(var timestamp: Long) {
  private val seconds_in_Day = (24*60*60)

  def size_in_seconds: Long = size.toMilliseconds / 1000

  // This is the base timestamp ... the epoch of the world with flink
  val base = 0L // TODO: currently set 0 to align with Unix Epoch

  def ts_start: Long = timestamp - (timestamp % size_in_seconds)

  def ts_stop: Long = ts_start + size_in_seconds - 1

  def i: Long = ( ts_start - base ) / size_in_seconds

  def num_slices_in(hr: Long): Long = (hr * 60 * 60) / size_in_seconds

  def num_slices_in_day: Long = num_slices_in(24)

  def j: immutable.IndexedSeq[Long] = {
    val k = num_slices_in_day
    (1L to ((i+2)/k)).map(n => (i+2 - n*k))
  }

  def start_time_of_day: Long = ts_start % seconds_in_Day
  def stop_time_of_day: Long = ts_stop % seconds_in_Day

  def predicting_for_time_of_day: Long = (start_time_of_day + 2*size_in_seconds) % seconds_in_Day
  def predicting_for_slice: Slice = Slice(size)(ts_start + 2*size_in_seconds)

  override def toString : String = {
    val str: StringBuilder = new StringBuilder

    str.append(size.toMilliseconds.toString)
    str.append(Constants.DELIMITER)
    str.append(ts_start.toString)
    str.append(Constants.DELIMITER)
    str.append(ts_stop.toString)

    str.toString()
  }
}
