package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.base.AbstractKeyGetter
import com.bhaskardivya.projects.smartgrid.model.{AverageWithKey, SensorEvent}
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * The accumulator is used to keep a running sum and a count. The [getResult] method
  * computes the average.
  */
class AverageAggregateWithKey(keyGetter: AbstractKeyGetter) extends AggregateFunction[SensorEvent, AverageWithKey, AverageWithKey]{

  override def createAccumulator(): AverageWithKey = AverageWithKey("", 0.0, 0L)

  override def add(value: SensorEvent, accumulator: AverageWithKey): AverageWithKey =
    AverageWithKey(
      keyGetter(value),
      accumulator.sum + value.value,
      accumulator.count + 1L
    )

  override def getResult(accumulator: AverageWithKey): AverageWithKey = accumulator

  override def merge(a: AverageWithKey, b: AverageWithKey): AverageWithKey = a + b
}
