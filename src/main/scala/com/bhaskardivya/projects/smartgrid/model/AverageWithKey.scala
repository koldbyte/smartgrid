package com.bhaskardivya.projects.smartgrid.model

import com.gotometrics.orderly.DoubleWritableRowKey
import org.apache.hadoop.io.DoubleWritable

case class AverageWithKey(var key: SensorKeyObject, var sum: Double, var count: Long, eventTimestamp: Long = Constants.KEY_NO_VALUE){
  val averageValue: Double = sum / count

  def add(that: AverageWithKey): AverageWithKey = {
    // To ensure that we do no propagate "-1" (Constants.KEY_NO_VALUE) (initial accumulator value)
    if(this.key.house_id > that.key.house_id)
      AverageWithKey(this.key, this.sum + that.sum, this.count + that.count, Math.max(this.eventTimestamp, that.eventTimestamp))
    else
      AverageWithKey(that.key, this.sum + that.sum, this.count + that.count, Math.max(this.eventTimestamp, that.eventTimestamp))
  }

  def +(that: AverageWithKey): AverageWithKey = {
    this.add(that)
  }

  def toHBaseColumnName(): String = {
    key.toColumnString()
  }

  def bytesRowKey(): Array[Byte] = {
    // Original Object that will be serialized
    val rowkeyVal: DoubleWritable = new DoubleWritable(toHBaseColumnValue().asInstanceOf[java.lang.Double])

    new DoubleWritableRowKey().serialize(rowkeyVal)
  }

  /**
    * Note that this will be stored as primitive double type of java
    * Otherwise, DoubleColumnInterpreter will not be able to read column value
    * @return
    */
  def toHBaseColumnValue(): Double = {
    //this.sum + Constants.DELIMITER + this.count
    averageValue
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
    AverageWithKey(SensorKeyObject.fromColumnString(column), sumValue, 1, Constants.KEY_NO_VALUE)
  }
}

object AverageWithKey {
  def reducer = {
    (a: AverageWithKey, b: AverageWithKey) => a+b
  }
}
