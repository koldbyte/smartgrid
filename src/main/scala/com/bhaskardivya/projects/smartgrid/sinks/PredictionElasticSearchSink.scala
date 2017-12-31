package com.bhaskardivya.projects.smartgrid.sinks

import com.bhaskardivya.projects.smartgrid.model.Prediction
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.client.Requests


object PredictionElasticSearchSink {
  def of(esIndex: String, esType: String): ElasticsearchSinkFunction[Prediction]= {
    new ElasticsearchSinkFunction[Prediction] {

      def createIndexRequest(element: Prediction): ActionRequest = {
        val json = element.toJSONString()

        return Requests.indexRequest
          .index(esIndex)
          .`type`(esType)
          .source(json)
      }

      override def process(element: Prediction, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        indexer.add(createIndexRequest(element))
      }
    }
  }

}
