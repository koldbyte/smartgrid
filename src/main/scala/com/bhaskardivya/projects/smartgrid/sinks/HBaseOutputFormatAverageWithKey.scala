package com.bhaskardivya.projects.smartgrid.sinks

import java.io.IOException

import com.bhaskardivya.projects.smartgrid.model.AverageWithKey
import org.apache.flink.api.common.io.OutputFormat
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * This class implements an OutputFormat for HBase.
  */
@SerialVersionUID(1L)
class HBaseOutputFormatAverageWithKey extends OutputFormat[AverageWithKey] {
  //private var configuration: Configuration = _
  private var connection: Connection = _
  private var table: Table = _
  private var taskNumber: String = _
  private var rowNumber = 0
  var tableName: String = _
  var columnFamily: String = _

  def of(tableName: String, columnFamily: String): HBaseOutputFormatAverageWithKey = {
    this.tableName = tableName
    this.columnFamily = columnFamily
    this
  }

  override def configure(parameters: org.apache.flink.configuration.Configuration) = {
    //configuration = HBaseConfiguration.create()
    //configuration.addResource("/opt/hbase/conf/hbase-site.xml")
    //configuration.set("hbase.zookeeper.quorum", "localhost")
    //configuration.set("hbase.zookeeper.property.clientPort", "2181")
    //configuration.set("zookeeper.znode.parent", "/hbase")
  }

  @throws[IOException]
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    connection = ConnectionFactory.createConnection()
    table = connection.getTable(TableName.valueOf(tableName))
    this.taskNumber = String.valueOf(taskNumber)
  }

  @throws[IOException]
  override def writeRecord(record: AverageWithKey): Unit = {
    val put = new Put(Bytes.toBytes(taskNumber + rowNumber))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(record.key), Bytes.toBytes(record.toHBaseColumnValue()))
    rowNumber += 1
    table.put(put)
  }

  @throws[IOException]
  override def close(): Unit = {
    table.close()
    connection.close()
  }
}