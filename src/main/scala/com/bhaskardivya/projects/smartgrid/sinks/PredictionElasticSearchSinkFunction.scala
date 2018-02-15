package com.bhaskardivya.projects.smartgrid.sinks

import com.bhaskardivya.projects.smartgrid.model.Prediction2
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.client.Requests

/**
  * Sink to ElasticSearch to store the prediction values
  * @param esIndex  ElasticSearch Index name
  * @param esType   ElasticSearch Index type
  */
class PredictionElasticSearchSinkFunction(esIndex: String, esType: String) extends ElasticsearchSinkFunction[Prediction2]{

  def createIndexRequest(element: Prediction2): ActionRequest = {
    val json = element.toJSONString()

    Requests.indexRequest
      .index(esIndex)
      .`type`(esType)
      .source(json)
  }

  override def process(element: Prediction2, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
    indexer.add(createIndexRequest(element))
  }

}
