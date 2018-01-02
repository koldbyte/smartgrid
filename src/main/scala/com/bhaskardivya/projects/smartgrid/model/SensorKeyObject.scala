package com.bhaskardivya.projects.smartgrid.model

import org.apache.sling.commons.json.JSONObject

class SensorKeyObject(var house_id: Long, var household_id: Long, var plug_id: Long) extends Serializable{

  override def toString: String = this.toColumnString()

  def toColumnString(): String = {
    val str: StringBuilder = new StringBuilder

    if(this.house_id > Constants.KEY_NO_VALUE)
      str.append(this.house_id.toString)

    if(this.household_id > Constants.KEY_NO_VALUE)
      str.append(Constants.DELIMITER)
      str.append(this.household_id)

    if(this.plug_id > Constants.KEY_NO_VALUE)
      str.append(Constants.DELIMITER)
      str.append(this.household_id)

    str.toString()
  }

  def toJSONString(): String = {
    toJSON().toString
  }

  def toJSON(): JSONObject = {
    val json: JSONObject = new JSONObject()

    if(this.house_id > Constants.KEY_NO_VALUE)
      json.put("house_id", this.house_id)

    if(this.household_id > Constants.KEY_NO_VALUE)
      json.put("household_id", this.household_id)

    if(this.plug_id > Constants.KEY_NO_VALUE)
      json.put("plug_id", this.plug_id)

    json
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[SensorKeyObject]

  override def equals(other: Any): Boolean = other match {
    case that: SensorKeyObject =>
      (that canEqual this) &&
        house_id == that.house_id &&
        household_id == that.household_id &&
        plug_id == that.plug_id
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(house_id, household_id, plug_id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object SensorKeyObject {

  def apply(house_id: Long): SensorKeyObject = {
    new SensorKeyObject(house_id, Constants.KEY_NO_VALUE, Constants.KEY_NO_VALUE)
  }

  def apply(house_id: Long, household_id: Long, plug_id: Long): SensorKeyObject ={
    new SensorKeyObject(house_id, household_id, plug_id)
  }

  def fromColumnString(columnValue: String): SensorKeyObject = {
    val fields = columnValue.split(Constants.DELIMITER)
    fields match {
      case array:Array[String] if array.length == 1 => new SensorKeyObject(array(0).toLong, Constants.KEY_NO_VALUE, Constants.KEY_NO_VALUE)
      case array:Array[String] if array.length == 2 => new SensorKeyObject(array(0).toLong, array(1).toLong, Constants.KEY_NO_VALUE)
      case array:Array[String] if array.length == 2 => new SensorKeyObject(array(0).toLong, array(1).toLong, array(2).toLong)
    }
  }
}

