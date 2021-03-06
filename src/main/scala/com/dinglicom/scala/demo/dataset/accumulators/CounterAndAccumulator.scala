package com.dinglicom.scala.demo.dataset.accumulators

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * 需求：统计tomcat日志当中exception关键字出现了多少次
 */
object CounterAndAccumulator {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    
    //统计tomcat日志当中exception关键字出现了多少次
    val sourceDataSet: DataSet[String] = env.readTextFile("file:///F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\tomcat\\catalina.out")
    
    sourceDataSet.map(new RichMapFunction[String,String] {
    
      var counter=new LongCounter()
    
      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("my-accumulator",counter)
      }
      override def map(value: String): String = {
        if(value.toLowerCase().contains("exception")){
          println("##:" + value)
          counter.add(1)
        }
        value
      }
    }).setParallelism(4).writeAsText("c:\\t4", WriteMode.OVERWRITE)
    
    val job=env.execute()
    //获取累加器，并打印累加器的值
    val a=job.getAccumulatorResult[Long]("my-accumulator")
    
    println(a)  
  }
}