package com.dinglicom.scala.demo.datastream

import com.dinglicom.scala.demo.common.Person
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import com.dinglicom.scala.demo.datastream.custom.CustomSinkToMysql

/**
  * <p/>Sink数据输出
  * <li>Description: Sink</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020/02/12 15:34</li>
  */
object DataStreamSinkToMysqlApp {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val personStream = text.map(new MapFunction[String, Person] {
      override def map(value: String): Person = {
        val spilt = value.split(",")

        Person(Integer.parseInt(spilt(0)), spilt(1), Integer.parseInt(spilt(2)))
      }
    })
    personStream.addSink(new CustomSinkToMysql)

    env.execute("DataStreamSinkToMysqlApp")

  }


}
