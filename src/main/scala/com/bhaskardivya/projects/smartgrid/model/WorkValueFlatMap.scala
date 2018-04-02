package com.bhaskardivya.projects.smartgrid.model

import com.bhaskardivya.projects.smartgrid.base.AbstractKeyGetter
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class WorkValueFlatMap(size: Time)(keyGetter: AbstractKeyGetter) extends FlatMapFunction[TwoWorkEvents, AverageWithKey]{
  override def flatMap(value: TwoWorkEvents, out: Collector[AverageWithKey]): Unit = {

    if(!value.isValid)
      return

    // TwoWorkEvents have records with the same key
    // Find the slices between the two WorkEvents
    // Generate AverageWithKey records for each of the slices

    val slice_range: Seq[Long] = getSliceRange(value)

    val averageLoad = getAverageLoad(value)

    // let's pre-create the objects so that we dont need to create it again for each collect call
    val sensorKeyObject = keyGetter(value.sensorEvent1)
    val average = Average(averageLoad, 1)


    slice_range.foreach (
      slice_index => {
        val averageWithKey = AverageWithKey(sensorKeyObject, Slice.from(size)(slice_index), average)
        out.collect(averageWithKey)
      }
    )

  }

  def getSliceRange(value: TwoWorkEvents) =  {
    val slice_range_a = Slice(size)(value.sensorEvent1.timestamp).i
    val slice_range_b = Slice(size)(value.sensorEvent2.timestamp).i

    if(slice_range_a > slice_range_b){
      slice_range_b to slice_range_a
    } else {
      slice_range_a to slice_range_b
    }
  }

  def getAverageLoad(value: TwoWorkEvents) = {
    //in kWH
    val work_diff = Math.abs(value.sensorEvent1.value - value.sensorEvent2.value) * 1000

    //in seconds
    val time_diff = Math.abs(value.sensorEvent1.timestamp - value.sensorEvent2.timestamp)

    val averageLoad = (work_diff.toDouble * 60 * 60) / time_diff

    averageLoad
  }
}
