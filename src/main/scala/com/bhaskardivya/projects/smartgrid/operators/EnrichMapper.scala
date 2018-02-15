package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.{AverageWithKey, MedianLoad, MedianLoadWithKey, Prediction2}
import com.tdunning.math.stats.TDigest
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichReduceFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

/**
  * Rich Mapper function to get the predicted load values from the median state and Average Loads
  */
class EnrichMapper extends RichFlatMapFunction[AverageWithKey, Prediction2]{

  private var digest: ValueState[TDigest] = _
  private var prediction2: Prediction2 = _

  override def open(parameters: Configuration): Unit = {
    val descriptor = new ValueStateDescriptor[TDigest]("median", createTypeInformation[TDigest])
    digest = getRuntimeContext.getState(descriptor)
  }

  override def flatMap(value: AverageWithKey, out: Collector[Prediction2]): Unit = {
    val currentDigest = digest.value()

    val medianLoad =
      if(currentDigest == null) {
        value.averageValue
      }else{
        currentDigest.quantile(0.5)
      }

    val prediction = (value.averageValue + medianLoad) / 2.0

    if (prediction2 == null) {
      prediction2 = Prediction2(value, MedianLoad(medianLoad), prediction)
    } else {
      prediction2.averageWithKey = value
      prediction2.medianLoad = MedianLoad(medianLoad)
      prediction2.predictedLoad = prediction
    }

    out.collect(prediction2)
  }
}
