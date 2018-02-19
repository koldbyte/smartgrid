package com.bhaskardivya.projects.smartgrid.model

import com.gotometrics.orderly.DoubleWritableRowKey
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.hadoop.io.DoubleWritable
import org.apache.flink.streaming.api.scala._

case class AverageWithKey(var key: SensorKeyObject, var slice: Slice, average: Average){
  def averageValue = average.avg

  def add(that: AverageWithKey): AverageWithKey = {
    AverageWithKey(this.key, this.slice, this.average + that.average)
  }

  def +(that: AverageWithKey): AverageWithKey = {
    this.add(that)
  }

  /* HBase related functions start */
  @deprecated
  def toHBaseColumnName(): String = {
    key.toColumnString
  }

  @deprecated
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
  @deprecated
  def toHBaseColumnValue(): Double = {
    //this.sum + Constants.DELIMITER + this.count
    averageValue
  }

  @deprecated
  def toHBaseLongColumnValue(): Long = {
    //this.sum + Constants.DELIMITER + this.count
    (averageValue * 1000).toLong
  }

  /*def fromHBaseColumnValue(column: String, value: String): AverageWithKey = {
    //val fields = value.split(Constants.DELIMITER)
    //AverageWithKey(column, fields(0).toDouble, fields(2).toLong)
    var sumValue = 0.0
    try {
      sumValue = value.toDouble
    }catch {
      case e: Exception => sumValue = 0.0
    }
    AverageWithKey(SensorKeyObject.fromColumnString(column), sumValue, 1, Constants.KEY_NO_VALUE)
  }*/
}

object AverageWithKey {
  def reducer = {
    (a: AverageWithKey, b: AverageWithKey) => a+b
  }

  def getInitialValue(): AverageWithKey = {
    new AverageWithKey(SensorKeyObject(-1), Slice(Time.milliseconds(-1))(-1), Average(0.0, 0) )
  }
}
