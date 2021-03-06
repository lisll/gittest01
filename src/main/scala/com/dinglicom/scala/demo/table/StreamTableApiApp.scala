package com.dinglicom.scala.demo.table

import org.apache.flink.streaming.api.scala.{DataStream,StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.common.functions.MapFunction
import com.dinglicom.scala.demo.common.Person   
    
/**
  * <p/>Table API
  * <li>Description: Stream Table</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020/02/14 10:30</li>
  */
object StreamTableApiApp {

  def main(args: Array[String]): Unit = {

    //获取flink运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //获取flink table运行时环境
    val tableEnv = StreamTableEnvironment.create(env)
    
    //获取DataStream数据流
    val text : DataStream[String] = env.socketTextStream("localhost", 9999)
    
    // 引入隐式转换(否则报错)
    import org.apache.flink.api.scala._

    val personStream = text.map(new MapFunction[String, Person] {
      override def map(value: String): Person = {
        val spilt = value.split(",")

        Person(Integer.parseInt(spilt(0)), spilt(1), Integer.parseInt(spilt(2)))
      }
    })    
    
    //将数据流转为Table对象
    val table = tableEnv.fromDataStream(personStream)
    //将数据流注册成一张表
    tableEnv.registerTable("person", table)
    
    val result = tableEnv.sqlQuery("select id,name,age from person")

    tableEnv.toAppendStream[(Int,String,Int)](result).print()

    env.execute("StreamTableApiApp")    
  }
}
