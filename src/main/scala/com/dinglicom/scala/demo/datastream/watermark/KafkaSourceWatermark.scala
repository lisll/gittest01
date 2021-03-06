package com.dinglicom.scala.demo.datastream.watermark

import net.sf.json.JSONObject
import com.dinglicom.scala.demo.utils.ConfigUtils
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import com.dinglicom.scala.demo.datastream.source.KafkaEventSchema

/**
  * <p/>
  * <li>Description: 设置自定义时间戳分配器和watermark发射器</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-16 10:00</li>
  */
object KafkaSourceWatermark {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000) //设置周期性watermark生成时间
    
    env.setParallelism(1) //设置并行度为1,默认并行度是当前机器的cpu数量
    
    import org.apache.flink.api.scala._
    //设置事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaConfig = ConfigUtils.apply("json")//json格式{fruit:"abc",number:1,time:1581947195}

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new
        KafkaEventSchema, kafkaConfig._2)
      .setStartFromLatest()
      .assignTimestampsAndWatermarks(new CustomWatermarkExtractor2) //设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置

    env
      .addSource(kafkaConsumer)
      //      .assignTimestampsAndWatermarks(CustomWatermarkExtractor)//设置自定义时间戳分配器和watermark发射器
      .keyBy(_.getString("fruit"))
      .window(TumblingEventTimeWindows.of(Time.seconds(10))) //滚动窗口，大小为10s
      //.allowedLateness(Time.seconds(10)) //允许10秒延迟
      .reduce(new ReduceFunction[JSONObject] { //对json字符串中key相同的进行聚合操作
        override def reduce(value1: JSONObject, value2: JSONObject): JSONObject = {
          val json = new JSONObject()
          json.put("fruit", value1.getString("fruit"))
          json.put("number", value1.getInt("number") + value2.getInt("number"))
          //println(value1.getInt("idx")+ "=" + value2.getInt("idx"))
          json
        }
      }).print()

    env.execute("KafkaSourceWatermarkTest")

  }

}
