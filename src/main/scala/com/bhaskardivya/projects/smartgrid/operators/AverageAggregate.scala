package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.{Average, SensorEvent}
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * The accumulator is used to keep a running sum and a count. The [getResult] method
  * computes the average.
  */
class AverageAggregate extends AggregateFunction[SensorEvent, Average, Average] {

  override def createAccumulator(): Average = Average(0.0, 0L)

  override def add(value: SensorEvent, accumulator: Average): Average =
    Average(accumulator.sum + value.value, accumulator.count + 1L)

  override def getResult(accumulator: Average): Average = accumulator

  override def merge(a: Average, b: Average): Average = a+b  //Average(a.sum + b.sum, a.count + b.count)
}
