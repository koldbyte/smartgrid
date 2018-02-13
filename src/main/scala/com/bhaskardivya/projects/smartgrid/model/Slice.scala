package com.bhaskardivya.projects.smartgrid.model

import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Class to represent a slice index of a size (aka window size)
  *
  * @param size       Window size
  * @param timestamp  Event timestamp from the record
  */
case class Slice(size: Time)(timestamp: Long) {

  lazy val size_in_seconds = size.toMilliseconds / 1000

  // This is the base timestamp ... the epoch of the world with flink
  val base = 0L // TODO: currently set 0 to align with Unix Epoch

  lazy val ts_start = timestamp - (timestamp % size_in_seconds)

  lazy val ts_stop = ts_start + size_in_seconds - 1

  lazy val i = ( ts_start - base ) / size_in_seconds

  def num_slices_in(hr: Long) = (hr * 60 * 60) / size_in_seconds

  val num_slices_in_day = num_slices_in(24)

  lazy val j = {
    val k = num_slices_in_day
    (1L to ((i+2)/k)).map(n => (i+2 - n*k))
  }
}
