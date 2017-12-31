package com.bhaskardivya.projects.smartgrid.sources

import com.bhaskardivya.projects.smartgrid.model.AverageWithKey
import org.apache.hadoop.hbase.client.coprocessor.{AggregationClient, DoubleColumnInterpreter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Scan

object HBaseMedianSource extends Serializable{
  val conf: Configuration = HBaseConfiguration.create()
  conf.set("hbase.rpc.timeout","1800000")
  conf.set("hbase.zookeeper.property.maxClientCnxns","0")
  conf.set("hbase.client.scanner.timeout.period", "1800000")
  //println("BD | Constructor | " + conf.toString)
  //Configuration.dumpConfiguration(conf, new BufferedWriter(new OutputStreamWriter(System.out)))
  val aggregationClient: AggregationClient = new AggregationClient(conf)

  def getMedian(table: String, columnFamily: String, avg: AverageWithKey): Double = {
    //println("BD | Get median called")
    val scan = new Scan()
    if(columnFamily != null && !columnFamily.isEmpty) {
      if (avg != null && avg.key!= null && !avg.key.isEmpty)
        scan.addColumn(columnFamily.getBytes(), avg.key.getBytes())
      else
        scan.addFamily(columnFamily.getBytes())
    }
    println("BD | Fetching Median " + table + " | " + columnFamily + " | " + avg.key)
    //println("BD | " + conf.toString)
    val median = aggregationClient.median(TableName.valueOf(table), new DoubleColumnInterpreter, scan)
    println("BD | Median Fetched " + table + " | " + columnFamily + " | " + avg.key)
    median
  }

}
