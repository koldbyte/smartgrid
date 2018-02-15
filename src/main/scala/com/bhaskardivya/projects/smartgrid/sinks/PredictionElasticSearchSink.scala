package com.bhaskardivya.projects.smartgrid.sinks

import java.net.{InetAddress, InetSocketAddress}

import com.bhaskardivya.projects.smartgrid.model.Prediction2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink

object PredictionElasticSearchSink {

  def apply(params: ParameterTool, esIndex: String, esIndexType: String): ElasticsearchSink[Prediction2] ={
    //Initialize Elastic search configuration
    val esClusterLocationIP = params.get("es.cluster.ip", "192.168.99.100")
    val esClusterLocationPort = params.getInt("es.cluster.port", 9300)
    val esFlushMaxActions = params.getInt("bulk.flush.max.actions", 100)

    val config = new java.util.HashMap[String, String]
    config.put("cluster.name", params.get("es.cluster.name", "docker-cluster"))
    // This instructs the sink to emit after every element, otherwise they would be buffered
    config.put("bulk.flush.max.actions", esFlushMaxActions.toString)

    val transportAddresses = new java.util.ArrayList[InetSocketAddress]

    /*
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), esClusterLocationPort))
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.17.0.2"), esClusterLocationPort))
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("0.0.0.0"), esClusterLocationPort))
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("localhost"), esClusterLocationPort))
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.99.100"), esClusterLocationPort))
    */

    transportAddresses.add(new InetSocketAddress(InetAddress.getByName(esClusterLocationIP), esClusterLocationPort))

    new ElasticsearchSink(config, transportAddresses, new PredictionElasticSearchSinkFunction(esIndex, esIndexType))
  }

}
