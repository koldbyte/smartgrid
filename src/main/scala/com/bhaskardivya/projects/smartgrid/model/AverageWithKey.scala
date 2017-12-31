package com.bhaskardivya.projects.smartgrid.model

case class AverageWithKey(var key: String, var sum: Double, var count: Long){

  def add(that: AverageWithKey): AverageWithKey = {
    AverageWithKey(this.key, this.sum + that.sum, this.count + that.count)
  }

  def +(that: AverageWithKey): AverageWithKey = {
    this.add(that)
  }


  def toHBaseColumnValue(): Double = {
    //this.sum + Constants.DELIMITER + this.count
    ( sum / count )
  }

  def fromHBaseColumnValue(column: String, value: String): AverageWithKey = {
    //val fields = value.split(Constants.DELIMITER)
    //AverageWithKey(column, fields(0).toDouble, fields(2).toLong)
    var sumValue = 0.0
    try {
      sumValue = value.toDouble
    }catch {
      case e: Exception => sumValue = 0.0
    }
    AverageWithKey(column, sumValue, 1)
  }
}

object AverageWithKey {
  def reducer = {
    (a: AverageWithKey, b: AverageWithKey) => a+b
  }
}
