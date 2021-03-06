package com.dinglicom.scala.demo.datastream.trigger

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import com.dinglicom.scala.demo.utils.ConfigUtils
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.windowing.time.Time

object CustomTriggerApp {
  
  def main(args: Array[String]): Unit = {
    
    //获取flink流式运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment 
    
    val kafkaConfig = ConfigUtils.apply("string")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
      .setStartFromLatest()  
      
    import org.apache.flink.api.scala._

    val stream = env
      .addSource(kafkaConsumer)
      .setParallelism(1)
      .map(new RichMapFunction[String, Int] {
        override def map(value: String): Int = {
          Integer.valueOf(value)
        }
      })
      .timeWindowAll(Time.seconds(30))
      .trigger(new CustomProcessTimeTrigger)
      .sum(0).print()
      
    env.execute("CustomTriggerTest")      
  }
}