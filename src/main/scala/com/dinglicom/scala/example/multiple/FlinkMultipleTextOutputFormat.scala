package com.dinglicom.scala.example.multiple

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.hadoop.fs.{FileSystem, Path}

class FlinkMultipleTextOutputFormat[K, V] extends MultipleTextOutputFormat[K, V] {
  /**
   * 因为saveAsHadoopFile是以key,value的形式保存文件，写入文件之后的内容也是，按照key value的形式写入，
   * k,v之间用空格隔开，这里我只需要写入value的值，不需要将key的值写入到文件中个，
   * 所以我需要重写该方法，让输入到文件中的key为空即可，
   * 当然也可以进行领过的变通，也可以重写generateActuralValue(key:Any,value:Any),根据自己的需求来实现
   */
  override def generateActualKey(key: K, value: V): K = {
    NullWritable.get().asInstanceOf[K]
  }
  
  /**
   * 该方法是负责自定义生成文件的文件名
   */
  override def generateFileNameForKeyValue(key: K, value: V, name: String): String = {
    //key.asInstanceOf[String]
    
    val keyArr = key.toString()
    if (keyArr.equals("citycode")) {
      "city_60/" + name
      //"city_60/app_city_60.sub."
    }
    else{
      "tac8_60/" + name
      //"tac8_60/app_tac8_60.sub."
    }
    
    /*
    val keyArr = key.toString().split("\\=")
    if (keyArr(0).equals("1")) {
      //"dw_iot_app_apn_mr_60/startdate=" + keyArr(1) + "/app_city_60.sub."
      "dw_iot_app_apn_mr_60/startdate=" + keyArr(1) + "/" + name
    }
    else{
      //"dw_iot_app_tac8_mr_60/startdate=" + keyArr(1) + "/app_tac_60.sub."
       "dw_iot_app_tac8_mr_60/startdate=" + keyArr(1) + "/" + name
    }
    * 
    */
  }
  /**
   * 该方法使用来检查我们输出的文件目录是否存在，源码中，是这样判断的，如果写入的父目录已经存在的话，则抛出异常
   * 在这里我们冲写这个方法，修改文件目录的判断方式，
   * 如果传入的文件写入目录已存在的话，直接将其设置为输出目录即可，不会抛出异常
   */
  override def checkOutputSpecs(ignored: FileSystem, job:JobConf){
    var outDir: Path = FileOutputFormat.getOutputPath(job)
    if (outDir != null) { 
      //注意下面的这两句，如果说你要是写入文件的路径是hdfs的话，下面的两句不要写，或是注释掉，
      //它俩的作用是标准化文件输出目录，根据我的理解是，他们是标准化本地路径，
      //写入本地的话，可以加上，本地路径记得要用file:///开头，比如file:///E:/a.txt
      //val fs: FileSystem = ignored
      //outDir = fs.makeQualified(outDir)
      FileOutputFormat.setOutputPath(job, outDir)
    }
  }
}