package com.bhaskardivya.projects.smartgrid.operators

import com.bhaskardivya.projects.smartgrid.model.{AverageWithKey, MedianLoad}
import com.bhaskardivya.projects.smartgrid.sources.HBaseMedianSource
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future


class HBaseAsyncFunction(table: String, columnFamily: String) extends AsyncFunction[AverageWithKey, (AverageWithKey, MedianLoad)]{

  override def asyncInvoke(input: AverageWithKey, resultFuture: ResultFuture[(AverageWithKey, MedianLoad)]): Unit = {

    // issue the asynchronous request, receive a future for result// issue the asynchronous request, receive a future for result
    val resultMedian: Future[MedianLoad] = Future[MedianLoad]{
      //val hBaseMedianSource: HBaseMedianSource = new HBaseMedianSource()
      val median = HBaseMedianSource.getMedian(table, columnFamily, input)
      println("Found Median | " + median.toString + " for " + table.toString + ", " + columnFamily.toString + " for key " + input.key)
      MedianLoad(median)
    }

    // set the callback to be executed once the request by the client is complete
    // the callback simply forwards the result to the result future
    resultMedian.onSuccess {
      case result: MedianLoad => resultFuture.complete(Iterable((input, result)));
    }

    resultMedian.onFailure{
      case t: Throwable => {
        println(t.getMessage)
        println(t.getStackTrace.toString)
      }
    }

  }
}
