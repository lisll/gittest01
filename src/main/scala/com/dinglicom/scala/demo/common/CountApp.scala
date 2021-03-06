package com.dinglicom.scala.demo.common

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * <p/>实现通过一个add操作累加最终的结果，在job执行后可以获取最终结果
  * <li>Description: 累加器的使用</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-14 14:53</li>
  * 最简单的累加器是counter(计数器)：你可以通过Accumulator.add(V value)这个方法进行递增。在任务的最后，flink会把所有的结果进行合并，然后把最终结果发送到client端。累加器在调试或者你想更快了解你的数据的时候是非常有用的
  * Flink现在有一下内置累加器，每个累加器都实现了Accumulator接口。(IntCounter, LongCounter和 DoubleCounter)
  */
object CountApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")


    val info = data.map(new RichMapFunction[String, String]() {
      // step1：定义计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // step2: 注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)
      }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })

    val filePath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\sink\\scala"
    info.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(2)
    val jobResult = env.execute("CounterApp")

    // step3: 获取计数器
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")

    println("num: " + num)
  }

}
