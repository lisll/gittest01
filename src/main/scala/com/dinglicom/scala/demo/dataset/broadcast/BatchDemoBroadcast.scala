package com.dinglicom.scala.demo.dataset.broadcast

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

object BatchDemoBroadcast {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    
    // 1、初始化数据
    val broadData = new ListBuffer[Tuple2[String, Int]]()
    broadData.append(("zs", 18))
    broadData.append(("ls", 20))
    broadData.append(("ww", 17))
    val tupleData = env.fromCollection(broadData)
    val toBroadcastData = tupleData.map(data => data._1+","+data._2)

    val text = env.fromElements("zs", "ls", "ww", "bb")
    val result = text.map(new RichMapFunction[String, String] {
      var allMap = Map[String, Int]()
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        import scala.collection.JavaConverters._
        // 3、获取数据(getBroadcastVariable[Map[String,Int]]报错java.lang.NumberFormatException: Not a version: 9)
        val listData = getRuntimeContext.getBroadcastVariable[String]("broadcastData").asScala
        //println("-------------------")
        for (data <- listData) {
          val fileds = data.split("\\,")
          allMap.put(fileds(0), fileds(1).toInt)
        }        
      }

      override def map(in: String): String = {
        //allMap.getOrElse(in, "_NULL")
        if(allMap.get(in).isEmpty)
          in + "_NULL"
        else
          in + "_" + allMap(in)
      }
    }).withBroadcastSet(toBroadcastData, "broadcastData")// 2、广播数据
    result.print();
    //toBroadcastData.print()
  }
}