package com.dinglicom.scala.demo.dataset.broadcast

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

/**
  * <p/>Broadcast广播变量
  * <li>Description: Broadcast</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020/02/12 11:33</li>
  */
object BroadcastTest {
  
 def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    
    import org.apache.flink.api.scala._
    
    val ds1 = env.fromElements("1", "2", "3", "4", "5")
    
    val ds2 = env.fromElements("a", "b", "c", "d", "e")

    ds1.map(new RichMapFunction[String, (String, String)] {
      private var ds2: Traversable[String] = null

      /**
        * 此时会出现一个异常：java集合和scala集合不兼容的问题
        */
      import scala.collection.JavaConverters._
        
      override def open(parameters: Configuration) {
        ds2 = getRuntimeContext.getBroadcastVariable[String]("broadCast").asScala
      }

      def map(t: String): (String, String) = {
        var result = ""
        for (broadVariable <- ds2) {
          result = result + broadVariable + " "
        }
        (t, result)
      }
    }).withBroadcastSet(ds2, "broadCast").print()
  }  
}