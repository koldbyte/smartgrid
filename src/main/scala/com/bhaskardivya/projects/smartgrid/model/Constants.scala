package com.bhaskardivya.projects.smartgrid.model

object Constants {
  //Sliding Window Parameters
  val SLIDING_INTERVAL = 30

  //HBase Table parameters
  val TABLE_1MIN = "table1min"
  val TABLE_5MIN = "table5min"
  val TABLE_15MIN = "table15min"
  val TABLE_60MIN = "table60min"
  val TABLE_120MIN = "table120min"

  val HOUSE_CF = "house_avg"
  val PLUG_CF = "plug_avg"

  val DELIMITER = "|"

  // Elastic search related constants
  val ES_INDEX_NAME = "smartgrid"

  val ES_INDEX_TYPE_1MIN = "prediction_1min"
  val ES_INDEX_TYPE_5MIN = "prediction_5min"
  val ES_INDEX_TYPE_15MIN = "prediction_15min"
  val ES_INDEX_TYPE_60MIN = "prediction_60min"
  val ES_INDEX_TYPE_120MIN = "prediction_120min"

  val ES_INDEX_TYPE_RAW = "raw"

  // Sensor Key Object
  val KEY_NO_VALUE: Long = -1
}
