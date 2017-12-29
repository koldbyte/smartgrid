package com.bhaskardivya.projects.smartgrid.model

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorEvent] {

  override def extractTimestamp(element: SensorEvent, previousElementTimestamp: Long): Long = element.getTimeMillis()

  override def checkAndGetNextWatermark(lastElement: SensorEvent, extractedTimestamp: Long): Watermark = {
    //TODO:
    null
  }
}
