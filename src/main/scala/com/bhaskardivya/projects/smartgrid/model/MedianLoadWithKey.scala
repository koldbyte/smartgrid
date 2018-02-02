package com.bhaskardivya.projects.smartgrid.model

import com.tdunning.math.stats.TDigest

case class MedianLoadWithKey(var key: SensorKeyObject, totalDigest: TDigest){
  val medianLoad : Double = totalDigest.quantile(0.5)
  def add(averageWithKey: AverageWithKey) = {
    this.totalDigest.add(averageWithKey.averageValue)
    this
  }

  def add(other: MedianLoadWithKey) = {
    this.totalDigest.add(other.totalDigest)
    if(this.key.house_id < other.key.house_id){
      this.key = other.key
    }
    this
  }

  def +(other: MedianLoadWithKey) = add(other)
}

object MedianLoadWithKey {
  def reducer = {
    (a: MedianLoadWithKey, b: MedianLoadWithKey) => a+b
  }
}