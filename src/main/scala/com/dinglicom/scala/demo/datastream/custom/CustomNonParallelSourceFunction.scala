package com.dinglicom.scala.demo.datastream.custom

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * <p/>
  * <li>Description: 自定义source非并行</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020/02/12 13:21</li>
  */
class CustomNonParallelSourceFunction extends SourceFunction[Long] {

  var count = 1L

  var isRunning = true

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
