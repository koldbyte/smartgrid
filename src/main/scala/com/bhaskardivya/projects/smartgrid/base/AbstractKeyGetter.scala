package com.bhaskardivya.projects.smartgrid.base

import com.bhaskardivya.projects.smartgrid.model.{SensorEvent, SensorKeyObject}

abstract class AbstractKeyGetter extends Serializable{
  def apply(sensorEvent: SensorEvent): SensorKeyObject
}
