package com.dinglicom.scala.demo.datastream.sideoutputs

import com.dinglicom.scala.demo.utils.ConfigUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
  * <p/>
  * <li>Description: 侧输出分流</li>
  * flink提供了侧输出这个功能，侧输出的输出类型可以与主流不同，可以有多个侧输出(sideOutput)，每个侧输出不同的类型。
  *     1. 定义OutputTag
  *     2. 使用特定的函数ProcessFunction、CoProcessFunction、ProcessWindowFunction、ProcessAllWindowFunction
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-21 13:28</li>
  */
object SideOutPutTest {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //选择设置事件时间和处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val kafkaConfig = ConfigUtils.apply("string") //

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,
      new SimpleStringSchema(), //自定义反序列化器
      kafkaConfig._2)
      .setStartFromLatest()

    import org.apache.flink.api.scala._

    val outputTag = OutputTag[String]("5>side_output<10") //side_output>5为描述信息
    val outputTag1 = OutputTag[String]("side_output>=10") //side_output>5为描述信息
    
    val mainStream = env.addSource(kafkaConsumer)
      .map(new RichMapFunction[String, Integer] {
        override def map(value: String): Integer = {
          Integer.valueOf(value)
        }
      })
      //为流中的每个元素调用该函数，并可以生成零或多个输出流。
      .process(new ProcessFunction[Integer, Integer] {
      override def processElement(value: Integer, ctx: ProcessFunction[Integer, Integer]#Context, out: Collector[Integer]): Unit = {
        if (value <= 5)
          out.collect(value) //小于5的数据正常输出
        else if(value> 5 && value<10)
          //ctx.output(outputTag, "sideout-" + String.valueOf(value)) //侧输出，可能有多个
          ctx.output(outputTag, "5>side_output<10:" + String.valueOf(value))
        else
          ctx.output(outputTag1, "side_output>=10:" + String.valueOf(value))
      }
    })
    mainStream.print()
    mainStream.getSideOutput(outputTag).print()
    mainStream.getSideOutput(outputTag1).print()
    env.execute("SideOutPutTest")
  }

}
