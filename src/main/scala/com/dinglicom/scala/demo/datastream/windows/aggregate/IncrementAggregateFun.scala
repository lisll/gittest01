package com.dinglicom.scala.demo.datastream.windows.aggregate

import com.dinglicom.scala.demo.utils.ConfigUtils
import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SessionWindowTimeGapExtractor}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.functions.ReduceFunction

/**
 * 增量聚合
 * 
 * 窗口中每进入一条数据，就进行一次计算
 * reduce(reduceFunction)
 * aggregate(aggregateFunction)
 * sum(),min(),max()
 */
object IncrementAggregateFun {
  
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConfig = ConfigUtils.apply("string")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,
      new SimpleStringSchema(), //自定义反序列化器
      kafkaConfig._2)
      .setStartFromLatest() //setStartFromEarliest() / setStartFromLatest()：设置从最早/最新位移处开始消费

    import org.apache.flink.api.scala._
    val aggregate = env
      .addSource(kafkaConsumer)
      .map(new RichMapFunction[String, (String, Long)] {
        override def map(value: String): (String, Long) = {
          ("1L", value.toLong)
        }
      })
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce(new ReduceFunction[(String, Long)] {
        override def reduce(value1: (String, Long), value2: (String, Long)): (String, Long) = {
          println("执行reduce操作:" + value1 + "," + value2)
          (value1._1, value1._2 + value2._2)
        }
      })
      //.aggregate(new AverageAggregateFunction)//(增量聚合)窗口中每进入一条数据，就进行一次计算
    aggregate.print()

    env.execute("IncrementAggregateFun")
  }
}