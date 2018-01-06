package com.bhaskardivya.projects.smartgrid.sinks

import java.io.IOException

import com.bhaskardivya.projects.smartgrid.model.AverageWithKey
import org.apache.flink.api.common.io.OutputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * This class implements an OutputFormat for HBase.
  */
@SerialVersionUID(1L)
class HBaseOutputFormatAverageWithKey extends OutputFormat[AverageWithKey] {
  private var configuration: Configuration = _
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
    configuration = HBaseConfiguration.create()
    configuration.addResource(new Path("/opt/hbase/conf/hbase-site.xml"))
    configuration.addResource(new Path("/opt/hadoop/conf/core-site.xml"))
    configuration.set("hbase.zookeeper.property.maxClientCnxns","0")
    //configuration.set("hbase.zookeeper.quorum", "localhost")
    //configuration.set("hbase.zookeeper.property.clientPort", "2181")
    //configuration.set("zookeeper.znode.parent", "/hbase")
  }

  @throws[IOException]
  override def open(taskNumber: Int, numTasks: Int): Unit = {
    connection = ConnectionFactory.createConnection(configuration)
    table = connection.getTable(TableName.valueOf(tableName))
    this.taskNumber = String.valueOf(taskNumber)
  }

  @throws[IOException]
  override def writeRecord(record: AverageWithKey): Unit = {
    val startTime = System.currentTimeMillis();
    // Make sure that the rowkey is sorted by the average values
    val put = new Put(Bytes.toBytes(taskNumber + rowNumber) ++ record.bytesRowKey())
    put.setDurability(Durability.SKIP_WAL)
    put.addColumn(
      Bytes.toBytes(columnFamily),
      Bytes.toBytes(record.toHBaseColumnName()),
      Bytes.toBytes(record.toHBaseColumnValue().asInstanceOf[java.lang.Double].doubleValue())
      //Bytes.toBytes(Predef.Long2long(record.toHBaseLongColumnValue()))
      //Bytes.toBytes(record.toHBaseLongColumnValue().asInstanceOf[java.lang.Long].longValue())
    )
    rowNumber += 1
    table.put(put)

    println(taskNumber +"-"+ rowNumber + " WriteRecord took " + (System.currentTimeMillis() - startTime) + "ms")
  }

  @throws[IOException]
  override def close(): Unit = {
    table.close()
    connection.close()
  }
}