package com.bhaskardivya.projects.smartgrid.model

import com.tdunning.math.stats.TDigest

case class MedianLoadWithKey(var key: SensorKeyObject, slice: Slice, digest: TDigest){
  def medianLoad : Double = digest.quantile(0.5)

  def add(averageWithKey: AverageWithKey) = {
    this.digest.add(averageWithKey.averageValue)
    this
  }

  def add(other: MedianLoadWithKey) = {
    this.digest.add(other.digest)
    this
  }

  def +(other: MedianLoadWithKey) = add(other)
}

object MedianLoadWithKey {
  def reducer = {
    (a: MedianLoadWithKey, b: MedianLoadWithKey) => a+b
  }

  def fromAverageWithKey(averageWithKey: AverageWithKey) = {
    val tDigest: TDigest = TDigest.createDigest(Constants.TDIGEST_COMPRESSION)
    tDigest.add(averageWithKey.averageValue)

    MedianLoadWithKey(averageWithKey.key, averageWithKey.slice, tDigest)
  }
}