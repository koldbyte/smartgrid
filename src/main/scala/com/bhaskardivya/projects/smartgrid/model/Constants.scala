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
}
