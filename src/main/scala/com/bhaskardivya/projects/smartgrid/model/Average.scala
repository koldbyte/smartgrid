package com.bhaskardivya.projects.smartgrid.model

import org.apache.flink.streaming.api.scala._

/**
  * Class to hold the sum and count
  * @param sum      Aggregation of load values
  * @param count    Number of load values
  */
case class Average(var sum: Double, var count: Long){
  def add(that: Average): Average = {
    Average(this.sum + that.sum, this.count + that.count)
  }

  def +(that: Average): Average = {
    this.add(that)
  }

  def avg: Double = {
    sum/count
  }
}
