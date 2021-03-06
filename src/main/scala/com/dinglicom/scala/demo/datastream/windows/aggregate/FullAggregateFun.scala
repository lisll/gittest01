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
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector

/**
 * 全量聚合
 * 等属于窗口的数据到齐，才开始进行聚合计算【可以实现对窗口内的数据进行排序等需求】
 * apply(windowFunction)
 * process(processWindowFunction)
 * processWindowFunction比windowFunction提供了更多的上下文信息
 */
object FullAggregateFun {

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
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new MyProcessWindowFunction)
      
    aggregate.print()

    env.execute("FullAggregateFun")
  } 
  
  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

    def process(key: String, context: Context, input: Iterable[(String, Long)], out: Collector[String]) = {
      println("执行process操作: * *")
      var count = 0L
      var sumVal = 0L
      //ProcessWindowFunction对窗口中的数据元进行计数的情况
      for (in <- input){
        count = count + 1
        sumVal = in._2 + sumVal
      }
      out.collect(s"Window ${context.window} count: $count sum: $sumVal")
    }
  }  
}