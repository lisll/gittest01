package com.dinglicom.scala.demo.datastream.windows

import com.dinglicom.java.demo.feature.trigger.CustomProcessingTimeTrigger
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
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import com.dinglicom.scala.demo.datastream.trigger.CustomProcessTimeTrigger2
import com.dinglicom.scala.demo.datastream.trigger.AverageAggregateFunction

/**
  * <p/>
  * <li>Description: 会话窗口具有动态间隙的处理时间进行</li>
  *
  * 在会话窗口中按活动会话分配器组中的数据元。与翻滚窗口和滑动窗口相比，会话窗口不重叠并且没有固定的开始和结束时间。
  * 相反，当会话窗口在一段时间内没有接收到数据元时，即当发生不活动的间隙时，会关闭会话窗口。会话窗口分配器可以配置
  * 静态会话间隙或 会话间隙提取器函数，该函数定义不活动时间段的长度。当此期限到期时，当前会话将关闭，后续数据元将分配给新的会话窗口。
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-16 13:00</li>
  * 
  * flink的TimeCharacteristic枚举定义了三类值，分别是ProcessingTime、IngestionTime、EventTime
  * ProcessingTime是以operator处理的时间为准，它使用的是机器的系统时间来作为data stream的时间;
  * IngestionTime是以数据进入flink streaming data flow的时间为准;
  * EventTime是以数据自带的时间戳字段为准，应用程序需要指定如何从record中抽取时间戳字段
  * 
  * 指定为EventTime的source需要自己定义event time以及emit watermark，或者在source之外通过assignTimestampsAndWatermarks在程序手工指定
  * 
  * Flink Streaming Windows操作
  * 参考网址：https://www.jianshu.com/p/4442c01a6112
  */
object SessionWindowsAggregate {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // env.getConfig.setAutoWatermarkInterval(1000) //watermark间隔时间

    //设置时间特征(EventTime、ProcessingTime、IngestionTime)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

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
          val splits = value.split(" ")
          (splits(0), splits(1).toLong)
        }
      })
      //.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .windowAll(EventTimeSessionWindows.withGap(Time.seconds(10)))
//      .windowAll(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[(String, Long)] {
//        override def extract(element: (String, Long)): Long = {
//          // 当发生不活动的间隙时间间隔长度10s
//          10000
//        }
//      }))
      .trigger(new CustomProcessTimeTrigger2)
      .aggregate(new AverageAggregateFunction)//(增量聚合)窗口中每进入一条数据，就进行一次计算
    /*  
    	因为session看窗口没有一个固定的开始和结束，他们的评估与滑动窗口和滚动窗口不同。
    	在内部，session操作为每一个到达的元素创建一个新的窗口，并合并间隔时间小于指定非活动间隔的窗口。
    	为了进行合并，session窗口的操作需要指定一个合并触发器(Trigger)和一个合并窗口函数(Window Function),
    	如:ReduceFunction或者WindowFunction(FoldFunction不能合并)
		*/
    //窗口函数可以是ReduceFunction, AggregateFunction, FoldFunction 或 ProcessWindowFunction其中之一
      
    // 当前会话窗口结束才会输出值
    aggregate
      .print()
      .setParallelism(1)

    env.execute("SessionWindowsAggregate")

  }
}
