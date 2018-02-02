package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.{AverageWithKey, MedianLoad, MedianLoadWithKey, Prediction}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Calculates the prediction value and output a fully enriched datum
  * @param entity         The key for which prediction is made ie.House or Plug
  * @param slidingWindow
  */
class PredictionFunction(entity: String, slidingWindow: Long) extends MapFunction[(AverageWithKey, MedianLoadWithKey), Prediction]{
  override def map(value: (AverageWithKey, MedianLoadWithKey)): Prediction = {
    val avg = value._1.averageValue
    val median = value._2.medianLoad

    val predictedValue: Double = (avg + median)/ 2.0

    Prediction(value._1, value._2, entity, slidingWindow, predictedValue)
  }
}
