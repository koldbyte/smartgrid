package com.bhaskardivya.projects.smartgrid.model

case class AverageWithKey(var key: String, var sum: Double, var count: Long){

  def add(that: AverageWithKey): AverageWithKey = {
    AverageWithKey(this.key, this.sum + that.sum, this.count + that.count)
  }

  def +(that: AverageWithKey): AverageWithKey = {
    this.add(that)
  }


  def toHBaseColumnValue(): String = {
    this.sum + Constants.DELIMITER + this.count
  }

  def fromHBaseColumnValue(column: String, value: String): AverageWithKey = {
    val fields = value.split(Constants.DELIMITER)
    AverageWithKey(column, fields(0).toDouble, fields(2).toLong)
  }
}

object AverageWithKey {
  def reducer = {
    (a: AverageWithKey, b: AverageWithKey) => a+b
  }
}
