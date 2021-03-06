package com.dinglicom.scala.demo.datastream.custom

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
  * <p/>自定义数据源
  * <li>Description: TODO</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020/02/12 13:21</li>
  */
class CustomRichParallelSourceFunction extends RichParallelSourceFunction[Long]{

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
