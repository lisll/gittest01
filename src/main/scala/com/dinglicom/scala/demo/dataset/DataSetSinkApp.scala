package com.dinglicom.scala.demo.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * <p/>Sink数据输出
  * <li>Description: Sink</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020/02/11 11:33</li>
  */
object DataSetSinkApp {

  def main(args: Array[String]): Unit = {

    val env=ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data=1 to 10
    val text=env.fromCollection(data)

    val filePath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\sink\\scala\\sink_test.txt"
    //可以写的任何文件系统
    text.writeAsText(filePath,WriteMode.OVERWRITE)

    env.execute("SinkApp")
  }
}
