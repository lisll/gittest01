package com.dinglicom.scala.demo.dataset

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
  * <p/>DataSource主要负责数据的读取
  * <li>Description: DataSource</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020/02/11 13:08</li>
  */
object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    
    //1.读取集合
    //fromCollection(env)
    //2.读取本地txt文件
    //textFile(env)
    //3.读取本地csv文件
    //csvFile(env);
    //4.递归读取文件
    //readRecuriseFiles(env);
    //5.读取压缩文件
    readCompressionFiles(env)
    
    /*
    val filePath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\dataset\\input.txt"
    val line: DataSet[String] = env.readTextFile(filePath)
    import org.apache.flink.api.scala._

    val value: DataSet[String] = line.flatMap(x => {
      x.split("\t")
    })
   //value.print()
   print("================================")
   line.flatMap(new MyFun).print()
   * 
   */
  }

  class MyFun extends FlatMapFunction[String, String] {
    override def flatMap(value: String, out: Collector[String]): Unit = {
      val s = value.split(" ")
      for (e <- s) {
        out.collect(e)
      }
    }
  }


  /**
    * 集合datasource
    *
    * @param env
    */
  def fromCollection(env: ExecutionEnvironment) = {

    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()
  }

  /**
    * 文件/文件夹datasource
    *
    * @param env
    */
  def textFile(env: ExecutionEnvironment): Unit = {
    //可以直接指定文件夹
    env.readTextFile("F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\dataset\\input.txt").print()
  }

  /**
    * csv  datasource
    *
    * @param env
    * 
    * 读取CSV还有以下常用参数，便于我们灵活配置
    * fieldDelimiter: String指定分隔记录字段的定界符。默认的字段分隔符是逗号','
    * lenient: Boolean启用宽大的解析，即忽略无法正确解析的行。默认情况下，宽松的分析是禁用的，无效行会引发异常。
    * includeFields: Array[Int]定义要从输入文件读取的字段（以及忽略的字段）。
    *                 默认情况下，将解析前n个字段（由types()调用中的类型数定义） Array(0, 3)
    * lineDelimiter: String指定单个记录的分隔符。默认的行定界符是换行符'\n'
    * pojoFields: Array[String]指定映射到CSV字段的POJO的字段。
    *              CSV字段的解析器会根据POJO字段的类型和顺序自动初始化。 pojoFields = Array("name", "age", "zipcode")
    * parseQuotedStrings: Character启用带引号的字符串解析。如果字符串字段的第一个字符是引号字符（不修剪前导或尾部空格）
    *                      ，则将字符串解析为带引号的字符串。带引号的字符串中的字段定界符将被忽略。
    *                      如果带引号的字符串字段的最后一个字符不是引号字符，则带引号的字符串解析将失败。
    *                      如果启用了带引号的字符串解析，并且该字段的第一个字符不是带引号的字符串，则该字符串将解析为未带引号的字符串。
    *                      默认情况下，带引号的字符串分析是禁用的。
    * ignoreComments: String指定注释前缀。以指定的注释前缀开头的所有行都不会被解析和忽略。默认情况下，不忽略任何行。
    * ignoreFirstLine: Boolean将InputFormat配置为忽略输入文件的第一行。默认情况下，不忽略任何行。
    */
  def csvFile(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val filePath1 = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\dataset\\people.csv"
    val filePath2 = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\dataset\\sales.csv"
    //[T]可以指定为tuple或者pojo case class,可以指定需要的列或在参数重指定 includedFields = Array(0,1)列
    
    //文件路径     是否忽略第一行
    //env.readCsvFile[(String, Int, String)](filePath, ignoreFirstLine = true).print()

    //注:只能取两列(Tuple2\k.v键值对)，Array(a,b)a<b，若a>b，去默认下标为0的列
    env.readCsvFile[(Int, String)](filePath2, includedFields = Array(1,2)).print()  
    
    //env.readCsvFile[People](filePath, ignoreFirstLine = true).print()
  }

  /**
    * 递归，嵌套文件 datasource
    *
    * @param env
    */
  def readRecuriseFiles(env: ExecutionEnvironment): Unit = {

    val filePath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\recursion"
    
    val parameters = new Configuration

    //开启递归
    parameters.setBoolean("recursive.file.enumeration", true)

    // 使用递归方式读取(目录下所有文件)数据
    env.readTextFile(filePath).withParameters(parameters).print()

  }

  /**
    * 读压缩文件
    *
    * @param env
    */
  def readCompressionFiles(env: ExecutionEnvironment): Unit = {

    //val filePath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\dataset\\tiny.txt.gz"
    val filePath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\gz\\lteu1_http_202003060859_512_pssig10-125_66875-0140597504.DAT.gz"
    //不支持snappy
    //val filePath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\dataset\\http-172.16.41.173-2020030412-5.1583297929560.snappy"
    //http-172.16.41.173-2020030412-5.1583297929560.snappy
    env.readTextFile(filePath).print()
  }


}

case class People(name:String,age:Integer,job:String)