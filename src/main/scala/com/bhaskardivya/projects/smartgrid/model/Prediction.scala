package com.bhaskardivya.projects.smartgrid.model

import java.util.Date

import org.apache.sling.commons.json.JSONObject


case class Prediction(averageWithKey: AverageWithKey, medianLoad: MedianLoadWithKey, key: String, slidingWindow: Long, predictedValue: Double){

  def toJSONString(): String = {
    toJSON().toString()
  }

  def toJSON(): JSONObject = {
    //main object
    val json = new JSONObject()

    //averageWithKey object
    val averageWithKeyJSON = averageWithKey.key.toJSON
    averageWithKeyJSON.put("sum", averageWithKey.sum)
    averageWithKeyJSON.put("count", averageWithKey.count)
    averageWithKeyJSON.put("avg", averageWithKey.averageValue)
    averageWithKeyJSON.put("eventTimestamp", averageWithKey.eventTimestamp)
    json.put("averageWithKey", averageWithKeyJSON)

    //medianLoad
    val medianLoadJSON = new JSONObject()
    medianLoadJSON.put("load", medianLoad.medianLoad)
    json.put("medianLoad", medianLoadJSON)

    //key or entity
    json.put("key", key)

    //sliding window duration
    json.put("slidingWindowDuration", slidingWindow)

    //Predicted value
    json.put("predictedValue", predictedValue)

    // Current Time
    json.put("timestamp", new Date().getTime)

    json
  }
}