package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.{AverageWithKey, MedianLoad, Prediction}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Calculates the prediction value and output a fully enriched datum
  * @param entity         The key for which prediction is made ie.House or Plug
  * @param slidingWindow
  */
class PredictionFunction(entity: String, slidingWindow: Long) extends MapFunction[(AverageWithKey, MedianLoad), Prediction]{
  override def map(value: (AverageWithKey, MedianLoad)): Prediction = {
    val avg = value._1.averageValue
    val median = value._2.load

    val predictedValue: Double = (avg + median)/ 2.0

    Prediction(value._1, value._2, entity, slidingWindow, predictedValue)
  }
}
