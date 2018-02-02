package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.{AverageWithKey, MedianLoadWithKey, SensorKeyObject}
import com.tdunning.math.stats.TDigest
import org.apache.flink.api.common.functions.AggregateFunction

class MedianAggregateWithKey extends AggregateFunction[AverageWithKey, MedianLoadWithKey, MedianLoadWithKey]{
  override def createAccumulator() = MedianLoadWithKey(SensorKeyObject(-1), TDigest.createDigest(100))

  override def add(value: AverageWithKey, accumulator: MedianLoadWithKey) = accumulator.add(averageWithKey = value)

  override def getResult(accumulator: MedianLoadWithKey) = accumulator

  override def merge(a: MedianLoadWithKey, b: MedianLoadWithKey) = a+b
}
