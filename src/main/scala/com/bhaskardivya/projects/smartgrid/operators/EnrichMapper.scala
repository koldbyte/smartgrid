package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.{AverageWithKey, MedianLoad, MedianLoadWithKey, Prediction}
import com.tdunning.math.stats.TDigest
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichReduceFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

/**
  * Rich Mapper function to get the predicted load values from the median state and Average Loads
  */
class EnrichMapper(stateName: String) extends RichFlatMapFunction[AverageWithKey, Prediction]{

  private var digest: MapState[Long, TDigest] = _
  private var prediction2: Prediction = _

  override def open(parameters: Configuration): Unit = {
    val descriptor = new MapStateDescriptor[Long, TDigest](stateName, createTypeInformation[Long], createTypeInformation[TDigest])
    digest = getRuntimeContext.getMapState(descriptor)
  }

  override def flatMap(value: AverageWithKey, out: Collector[Prediction]): Unit = {
    // Get the TDigest Object for the SensorKey and slice predicting for
    val currentDigest = digest.get(value.slice.predicting_for_time_of_day)

    // Get the Median Load Value
    val medianLoad =
      if(currentDigest == null) {
        val previousDigest =  digest.get(value.slice.predicting_previous_slice)
        if(previousDigest == null)
          value.averageValue
        else
          previousDigest.quantile(0.5)
      }else{
        currentDigest.quantile(0.5)
      }

    // Calculate the load prediction
    val prediction = (value.averageValue + medianLoad) / 2.0

    // Update the Slice index for prediction
    //TODO: Testing //value.slice = value.slice.predicting_for_slice

    // Create the final Prediction Object to be collected
    if (prediction2 == null) {
      prediction2 = Prediction(value, MedianLoad(medianLoad), prediction)
    } else {
      prediction2.averageWithKey = value
      prediction2.medianLoad = MedianLoad(medianLoad)
      prediction2.predictedLoad = prediction
    }

    if(prediction > 1e-6) //
      out.collect(prediction2)
  }
}
