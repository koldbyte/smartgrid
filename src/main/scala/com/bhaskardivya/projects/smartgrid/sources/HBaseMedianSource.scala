package com.bhaskardivya.projects.smartgrid.sources

import com.bhaskardivya.projects.smartgrid.model.AverageWithKey
import org.apache.hadoop.hbase.client.coprocessor.{AggregationClient, DoubleColumnInterpreter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan}

class HBaseMedianSource extends Serializable{
  private var connection: Connection = _
  private var conf: Configuration = _
  private var aggregationClient: AggregationClient = _

  def HBaseMedianSource() = {
    println("BD | HBaseMedianSource | Initialized")
    //connection = ConnectionFactory.createConnection()
    //conf = connection.getConfiguration
    conf = new Configuration()

    println("BD | HBaseMedianSource | Initialization complete")
    println(conf.toString)
  }

  def getMedian(table: String, columnFamily: String, avg: AverageWithKey): Double = {
    println("BD | Get median called")
    val scan = new Scan()
    if(columnFamily != null && !columnFamily.isEmpty) {
      if (avg != null && avg.key!= null && !avg.key.isEmpty)
        scan.addColumn(columnFamily.getBytes(), avg.key.getBytes())
      else
        scan.addFamily(columnFamily.getBytes())
    }
    println("BD | Fetching Median " + table + " | " + columnFamily + " | " + avg.key)
    println(conf.toString)
    aggregationClient = new AggregationClient(conf)
    val median = aggregationClient.median(TableName.valueOf(table), new DoubleColumnInterpreter, scan)
    println("BD | Median Fetched " + table + " | " + columnFamily + " | " + avg.key)
    median
  }

}
