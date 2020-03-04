package com.bhaskardivya.projects.smartgrid.model

case class TwoWorkEvents(sensorEvent1: SensorEvent, sensorEvent2: SensorEvent) {
  def isValid: Boolean = sensorEvent1 != null && sensorEvent2 != null
}
