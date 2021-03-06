package com.dinglicom.scala.demo.dataset.distribute_cache

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import scala.collection.mutable.ListBuffer
import org.apache.flink.api.scala.createTypeInformation

/**
  * <p/>
  * <li>Description: DataSet 分布式缓存</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-14 14:15</li>
  * step1: 注册一个本地/HDFS文件
  * cache.txt
  * pig
  * hive
  * tez
  * flink
  * spark
  * 
  * step2：在open方法中获取到分布式缓存的内容即可
  * </li>
	*/
object DistributedCacheApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    
    val filePath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\feature\\cache.txt"

    // step1: 注册一个本地/HDFS文件
    env.registerCachedFile(filePath, "pk-scala-dc")

    // 隐式转换
    import org.apache.flink.api.scala._
    
    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

    data.map(new RichMapFunction[String, String] {
      var list1 = new ListBuffer[String]
      // step2：在open方法中获取到分布式缓存的内容即可
      override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache().getFile("pk-scala-dc")

        val lines = FileUtils.readLines(dcFile) // java

        /**
          * 此时会出现一个异常：java集合和scala集合不兼容的问题
          */
        import scala.collection.JavaConverters._
        for (ele <- lines.asScala) {
          println(ele)
          list1.append(ele)
        }
      }
      
      override def map(value: String): String = {
        var rt:String = ""
        if(list1.contains(value))
          rt = value
        rt
      }
    }).filter(_.nonEmpty).print()
  }

}
