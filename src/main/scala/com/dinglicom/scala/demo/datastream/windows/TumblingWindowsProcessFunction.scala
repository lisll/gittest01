package com.dinglicom.scala.demo.datastream.windows

import com.dinglicom.scala.demo.utils.ConfigUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import com.dinglicom.scala.demo.datastream.trigger.CustomProcessTimeTrigger2

/**
  * <p/>
  * <li>Description: 统计一个window中元素个数，此外，还将window的信息添加到输出中。</li>
  * 使用ProcessWindowFunction来做简单的聚合操作，如:计数操作，性能是相当差的。
  * 将ReduceFunction跟ProcessWindowFunction结合起来，来获取增量聚合和添加到ProcessWindowFunction中的信息，性能更好
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-15 15:30</li>
  */
object TumblingWindowsProcessFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000) //watermark间隔时间

    //设置事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    val kafkaConfig = ConfigUtils.apply("string")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,
      new SimpleStringSchema(),
      kafkaConfig._2)
      .setStartFromLatest()

    import org.apache.flink.api.scala._
    val process = env
      .addSource(kafkaConsumer)
      .map(new RichMapFunction[String, (String, Long)] {
        override def map(value: String): (String, Long) = {
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(30)) //滚动窗口：30秒为窗口时间
      .trigger(new CustomProcessTimeTrigger2)
      .process(new MyProcessWindowFunction)
      //触发器：满足条件-触发窗口函数
      //窗口函数：到达窗口时间-触发窗口函数
      
    process.print()
    env.execute("TumblingWindowsProcessFunction")
  }

  /**
    * 注意ProcessWindowFunction的包！！！！！
    *
    * 获取包含窗口的所有数据元的Iterable，以及可访问时间和状态信息的Context对象，这使其能够提供比其他窗口函数更多的灵活性。
    * 这是以性能和资源消耗为代价的，因为数据元不能以递增方式聚合，而是需要在内部进行缓冲，直到窗口被认为已准备好进行处理。
    *
    * ProcessWindowFunction可以与ReduceFunction，AggregateFunction或FoldFunction以递增地聚合元数据。
    * 当窗口关闭时，ProcessWindowFunction将提供聚合结果。这允许它在访问附加窗口元信息的同时递增地计算窗口ProcessWindowFunction。
    *
    */
  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

    def process(key: String, context: Context, input: Iterable[(String, Long)], out: Collector[String]) = {
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
