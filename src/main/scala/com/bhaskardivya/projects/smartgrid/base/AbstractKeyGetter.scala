package com.bhaskardivya.projects.smartgrid.base

import com.bhaskardivya.projects.smartgrid.model.SensorEvent

abstract class AbstractKeyGetter extends Serializable{
  def apply(sensorEvent: SensorEvent): String
}
