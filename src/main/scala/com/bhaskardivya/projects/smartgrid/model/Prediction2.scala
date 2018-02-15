package com.bhaskardivya.projects.smartgrid.model

import java.util.Date

import org.apache.sling.commons.json.JSONObject

case class Prediction2(var averageWithKey: AverageWithKey, var medianLoad: MedianLoad, var predictedLoad: Double) {

  def toJSONString(): String = {
    toJSON().toString()
  }

  def toJSON(): JSONObject = {
    //main object
    val json = new JSONObject()

    //averageWithKey object
    val averageWithKeyJSON = averageWithKey.key.toJSON
    averageWithKeyJSON.put("sum", averageWithKey.average.sum)
    averageWithKeyJSON.put("count", averageWithKey.average.count)
    averageWithKeyJSON.put("avg", averageWithKey.averageValue)
    averageWithKeyJSON.put("eventTimestamp", averageWithKey.slice.timestamp)
    json.put("averageWithKey", averageWithKeyJSON)

    //medianLoad
    val medianLoadJSON = new JSONObject()
    medianLoadJSON.put("load", medianLoad.load.formatted("%.3f").toFloat)
    json.put("medianLoad", medianLoadJSON)

    //key or entity
    json.put("house_id", averageWithKey.key.house_id)
    json.put("household_id", averageWithKey.key.household_id)
    json.put("plug_id", averageWithKey.key.plug_id)

    //sliding window duration
    json.put("slidingWindowDuration", averageWithKey.slice.size.toMilliseconds)
    json.put("slice-start", averageWithKey.slice.ts_start)
    json.put("slice-stop", averageWithKey.slice.ts_stop)

    //Predicted value
    json.put("predictedValue", predictedLoad)

    // Current Time
    json.put("current-timestamp", System.currentTimeMillis)

    json
  }
}
