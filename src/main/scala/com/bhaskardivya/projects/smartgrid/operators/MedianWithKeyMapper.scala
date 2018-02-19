package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model._
import com.tdunning.math.stats.TDigest
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * A Rich flatMap function to keep a running TDigest object for each key and each starting minutes of a day.
  */
class MedianWithKeyMapper extends RichFlatMapFunction[AverageWithKey, AverageWithKey]{

  private var digest: MapState[Long, TDigest] = _

  override def open(parameters: Configuration): Unit = {
    val descriptor = new MapStateDescriptor[Long, TDigest]("median", createTypeInformation[Long], createTypeInformation[TDigest])
    descriptor.setQueryable("median-query")
    digest = getRuntimeContext.getMapState(descriptor)
  }

  override def flatMap(value: AverageWithKey, out: Collector[AverageWithKey]): Unit = {
    val key = value.slice.start_time_of_day
    var currentDigest = digest.get(key)

    if(currentDigest == null){
      currentDigest = TDigest.createDigest(Constants.TDIGEST_COMPRESSION)
    }

    currentDigest.add(key)

    digest.put(key, currentDigest)

    out.collect(value)
  }
}
