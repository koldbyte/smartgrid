package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model._
import org.apache.flink.api.common.functions.ReduceFunction

/**
  * The reducer function for AverageWithKey object having same keys
  */
class AverageWithKeyReducer extends ReduceFunction[AverageWithKey]{
  override def reduce(a: AverageWithKey, b: AverageWithKey): AverageWithKey = AverageWithKey.reducer(a,b)
}
