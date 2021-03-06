package com.dinglicom.scala.example.multiple

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
 
/**
 * 多文件输出 
 */
object TestMultiple {
  class IteblogMultipleTextOutputFormat[K, V] extends MultipleTextOutputFormat[K, V] {
    override def generateActualKey(key: K, value: V): K =
      NullWritable.get().asInstanceOf[K]
 
    override def generateFileNameForKeyValue(key: K, value: V, name: String): String =
      key.asInstanceOf[String]
  }
 
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
 
    val multipleTextOutputFormat = new IteblogMultipleTextOutputFormat[String, String]()
    val jc = new JobConf()
    FileOutputFormat.setOutputPath(jc, new Path("F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\dataset\\out"))
    val format = new HadoopOutputFormat[String, String](multipleTextOutputFormat, jc)
    val batch = env.fromCollection(List(("A", "1"), ("A", "2"), ("A", "3"),
      ("B", "1"), ("B", "2"), ("C", "1"), ("D", "2")))
    batch.output(format)
    env.execute("MultipleTextOutputFormat")
  }
}