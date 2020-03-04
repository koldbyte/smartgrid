package com.bhaskardivya.projects.smartgrid.util

import com.tdunning.math.stats.TDigest
import java.io.Serializable

@SerialVersionUID(1L)
class TDigestMedian() extends Serializable {
  private var totalDigest: TDigest = _

  this.setTotalDigest(TDigest.createDigest(100))

  def getTotalDigest: TDigest = totalDigest

  def setTotalDigest(totalDigest: TDigest) {
    this.totalDigest = totalDigest
  }

  def addDigest(digest: Double): Unit = this.totalDigest.add(digest)

  def getMedian: Double = this.totalDigest.quantile(0.5)
}