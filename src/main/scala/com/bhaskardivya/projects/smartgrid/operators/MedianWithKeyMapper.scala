package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model._
import com.tdunning.math.stats.TDigest
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * A Rich flatMap function to keep a running TDigest object for each key and each starting minutes of a day.
  */
class MedianWithKeyMapper extends RichFlatMapFunction[AverageWithKey, AverageWithKey]{

  private var digest: ValueState[TDigest] = _

  override def open(parameters: Configuration): Unit = {
    val descriptor = new ValueStateDescriptor[TDigest]("median", createTypeInformation[TDigest])
    descriptor.setQueryable("median-query")
    digest = getRuntimeContext.getState(descriptor)
  }

  override def flatMap(value: AverageWithKey, out: Collector[AverageWithKey]): Unit = {

    var currentDigest = digest.value()

    if(currentDigest == null){
      currentDigest = TDigest.createDigest(Constants.TDIGEST_COMPRESSION)
    }

    currentDigest.add(value.averageValue)

    digest.update(currentDigest)

    out.collect(value)
  }
}
