package com.dinglicom.scala.demo.dataset.broadcast

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * dataStream当中的广播分区:
 * 注意：
 * 将数据广播给所有的分区，数据可能会被重复处理，
 * 一般用于某些公共的配置信息读取，不会涉及到更改的数据
 * 
 * demo:Flink使用广播实现配置动态更新
 * https://blog.csdn.net/qq_31866793/article/details/95939116
 */
object DataStreamBroadCast {
  
  def main(args: Array[String]): Unit = {
    
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    
    // 配置处理环境的并发度为4
    environment.setParallelism(4)
    
    import org.apache.flink.api.scala._
    val result: DataStream[String] = environment.fromElements("hello").setParallelism(1)
    val resultValue: DataStream[String] = result.broadcast.map(x => {
      println(x)
      x
    })
    
    resultValue.print()
    environment.execute()
  }
}