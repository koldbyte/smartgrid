package com.bhaskardivya.projects.smartgrid.model

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.sling.commons.json.JSONObject


case class Prediction(averageWithKey: AverageWithKey, medianLoad: MedianLoad, key: String, slidingWindow: Time, predictedValue: Double){

  def toJSONString(): String = {
    toJSON().toString()
  }

  def toJSON(): JSONObject = {
    //main object
    val json = new JSONObject()

    //averageWithKey object
    val averageWithKeyJSON = new JSONObject()
    averageWithKeyJSON.put("key", averageWithKey.key)
    averageWithKeyJSON.put("sum", averageWithKey.sum)
    averageWithKeyJSON.put("count", averageWithKey.count)
    averageWithKeyJSON.put("avg", averageWithKey.sum / averageWithKey.count)
    json.put("averageWithKey", averageWithKeyJSON)

    //mediaLoad
    val medianLoadJSON = new JSONObject()
    medianLoadJSON.put("load", medianLoad.load)
    json.put("medianLoad", medianLoadJSON)

    //key or entity
    json.put("key", key)

    //sliding window duration
    json.put("slidingWindowDuration", slidingWindow.toMilliseconds.toString)

    //Predicted value
    json.put("predictedValue", predictedValue)

    json
  }
}