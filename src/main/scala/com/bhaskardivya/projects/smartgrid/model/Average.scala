package com.bhaskardivya.projects.smartgrid.model

case class Average(var sum: Double, var count: Long){
  def add(that: Average): Average = {
    Average(this.sum + that.sum, this.count + that.count)
  }

  def +(that: Average): Average = {
    this.add(that)
  }
}
