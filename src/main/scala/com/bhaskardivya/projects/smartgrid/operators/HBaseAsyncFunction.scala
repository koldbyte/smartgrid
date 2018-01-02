package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.{AverageWithKey, MedianLoad}
import com.bhaskardivya.projects.smartgrid.sources.HBaseMedianSource
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future
import org.slf4j.LoggerFactory
import org.slf4j.Logger

class HBaseAsyncFunction(table: String, columnFamily: String) extends AsyncFunction[AverageWithKey, (AverageWithKey, MedianLoad)]{
  val LOG :Logger = LoggerFactory.getLogger("HBaseAsyncFunction")

  override def asyncInvoke(input: AverageWithKey, resultFuture: ResultFuture[(AverageWithKey, MedianLoad)]): Unit = {

    // issue the asynchronous request, receive a future for result
    val resultMedian: Future[MedianLoad] = Future[MedianLoad]{
      val median = HBaseMedianSource.getMedian(table, columnFamily, input)
      MedianLoad(median)
    }

    // set the callback to be executed once the request by the client is complete
    // the callback simply forwards the result to the result future
    resultMedian.onSuccess {
      case result: MedianLoad => {
        LOG.info("Callback onSuccess Fetch Median - Got {}", result.load)
        resultFuture.complete(Iterable((input, result)))
      };
    }

    resultMedian.onFailure{
      case t: Throwable => {
        LOG.error("An error occurred fetching Median value from HBase - {}", t)
        //LOG.error("An e occurred.", "error", t)
      }
    }

  }
}
