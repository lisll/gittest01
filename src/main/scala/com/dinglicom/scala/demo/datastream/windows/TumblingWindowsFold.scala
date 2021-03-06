package com.dinglicom.scala.demo.datastream.windows

import com.dinglicom.scala.demo.utils.ConfigUtils
import org.apache.flink.api.common.functions.{FoldFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import com.dinglicom.scala.demo.datastream.trigger.CustomProcessTimeTrigger2

/**
  * <p/>
  * <li>Description: 滚动窗口的FoldFunction聚合函数</li>
  * FoldFunction指定窗口的输入数据元如何与输出类型的数据元组合
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-15 14:40</li>
  */
object TumblingWindowsFold {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000) //watermark间隔时间

    //设置事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val kafkaConfig = ConfigUtils.apply("string")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,
      new SimpleStringSchema(),
      kafkaConfig._2)
      .setStartFromLatest()


    import org.apache.flink.api.scala._
    val fold = env
      .addSource(kafkaConsumer)
      .map(new RichMapFunction[String, (String, Long)] {
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })
      .keyBy(0)//大多数情况下可以直接使用timewindow来替换window
      //.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(10)))
      .timeWindow(Time.seconds(15))
      .trigger(new CustomProcessTimeTrigger2)
      .fold(100L){(acc, v) => acc + v._2}

    fold.print()
    env.execute("TumblingWindowsFold")

  }
}
