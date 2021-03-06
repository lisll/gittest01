package com.dinglicom.scala.demo.datastream.trigger

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * <p/>
  * <li>Description: 累加器</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-15 15:55</li>
  */
class AverageAggregateFunction extends AggregateFunction[(String, Long), (Long, Long), Double] {
  
    /**
      * 创建一个新的累加器，启动一个新的聚合
      *
      * @return
      */
    override def createAccumulator() = {
      (0L, 0L)
    }

    /**
      * 将给定的输入值添加到给定的累加器，返回new accumulator值
      *
      * @param value
      * @param accumulator(聚合值，聚合元素个数)
      * @return
      */
    override def add(value: (String, Long), accumulator: (Long, Long)) = {
      //      println("触发: add  \t"+(accumulator._1 + value._2, accumulator._2 + 1L))
      (accumulator._1 + value._2, accumulator._2 + 1L)
    }

    /**
      * 从累加器获取聚合的结果
      *
      * @param accumulator
      * @return
      */
    override def getResult(accumulator: (Long, Long)) = {
      println("触发: getResult 累加计算结果 \t" + accumulator._1 + "---->" + accumulator._2)
      accumulator._1
    }

    /**
      * 合并两个累加器，返回具有合并状态的累加器
      *
      * @param a
      * @param b
      * @return
      */
    override def merge(a: (Long, Long), b: (Long, Long)) = {
      //      println("触发: merge  \t")
      (a._1 + b._1, a._2 + b._2)
    }
}
