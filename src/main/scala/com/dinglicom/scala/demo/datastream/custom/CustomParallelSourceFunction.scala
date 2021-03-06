package com.dinglicom.scala.demo.datastream.custom

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
  * <p/>
  * <li>Description: 自定义source能够并行</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020/02/12 13:21</li>
  */
class CustomParallelSourceFunction extends ParallelSourceFunction[Long] {

  var isRunning = true
  var count = 1L

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {

    isRunning = false
  }
}
