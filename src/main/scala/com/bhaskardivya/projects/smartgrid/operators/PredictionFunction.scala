package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.{AverageWithKey, MedianLoad, Prediction}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Calculates the prediction value and output a fully enriched datum
  * @param entity
  * @param slidingWindow
  */
class PredictionFunction(entity: String, slidingWindow: Long) extends MapFunction[(AverageWithKey, MedianLoad), Prediction]{
  override def map(value: (AverageWithKey, MedianLoad)): Prediction = {
    //AverageWithKey(0,100314.02700000002,29),MedianLoad(3505.088802675305)
    val avg = value._1.sum / value._1.count
    val median = value._2.load

    val predictedValue: Double = (avg + median)/ 2.0

    Prediction(value._1, value._2, entity, slidingWindow, predictedValue)
  }
}
