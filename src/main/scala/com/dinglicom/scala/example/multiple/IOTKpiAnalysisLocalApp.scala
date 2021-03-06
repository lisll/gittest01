package com.dinglicom.scala.example.multiple

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.java.utils.ParameterTool
import org.slf4j.LoggerFactory
import com.dinglicom.scala.example.Ltezc_http_220
import org.apache.flink.util.Collector

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}
import org.apache.flink.api.scala.operators.ScalaCsvOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat

/**
 * 物联网指标分析系统
 * 功能： 离线物联网指标数据分析
 * 日期：2020-03-04 14:00
 * 1.输出到HDFS
 * 2.iotKpiAnalysisApp.jar --date <date> --output <path>
 */
object IOTKpiAnalysisLocalApp {

  def main(args: Array[String]): Unit = {
    
    val logger = LoggerFactory.getLogger("TrafficAnalysis")
  
    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)
    
    val startdate = params.get("date")

    val filePath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\dataset\\gz"
    
    val line: DataSet[String] = env.readTextFile(filePath)
    
    //隐式转换(不引入报错)
    import org.apache.flink.api.scala._

    //2.数据清洗
    val logData = line.map(x => {
      val fileds = x.split("\\|")
      var u16citycode, u8rat, s8apn, u32tac8 = ""
      var outVal = new StringBuffer(1024)
      try {
        /*
        //维度
        u16citycode = fileds(Ltec1_http_cs.u16citycode)
        u8rat = fileds(Ltec1_http_cs.u8rat)
        s8apn = fileds(Ltec1_http_cs.s8apn).toLowerCase()
        u32tac8 = fileds(Ltec1_http_cs.u32tac8)
				*/
        u16citycode = fileds(Ltezc_http_220.u16citycode)
        u8rat = fileds(Ltezc_http_220.u8rat)
        s8apn = fileds(Ltezc_http_220.s8apn).toLowerCase()
        u32tac8 = fileds(Ltezc_http_220.u32tac8)        
        val s8imsi = fileds(Ltezc_http_220.u64imsi)
        //指标
        var http_req_cnt = 0L
        var http_success_cnt = 0L
        var http_success_delay = 0L
        var http_response_cnt = 0L
        var http_response_delay = 0L
        var http_ul_flow = 0L
        var http_dl_flow = 0L
        var active_user = "0"
        //测试集群
        /*
        val u32ultraffic = fileds(Ltec1_http_cs.u32ultraffic).toLong
        val u32dltraffic = fileds(Ltec1_http_cs.u32dltraffic).toLong
        val u16HttpWapStat = fileds(Ltec1_http_cs.u16httpwapstat).toLong
        val u32httpfirstrespondtime = fileds(Ltec1_http_cs.u32httpfirstrespondtime).toLong
        val u32httplastpackettime = fileds(Ltec1_http_cs.u32httplastpackettime).toLong
				*/
        //现网集群
        val u32ultraffic = fileds(Ltezc_http_220.u32ultraffic).toLong
        val u32dltraffic = fileds(Ltezc_http_220.u32dltraffic).toLong
        val u16HttpWapStat = fileds(Ltezc_http_220.u16itemrespondcode).toLong
        val u32httpfirstrespondtime = fileds(Ltezc_http_220.u32httpfirstrespondtime).toLong
        val u32httplastpackettime = fileds(Ltezc_http_220.u32httplastpackettime).toLong
        
        http_req_cnt = 1
        http_ul_flow = u32ultraffic
        http_dl_flow = u32dltraffic
        if (u16HttpWapStat > 0 && u16HttpWapStat < 400 && u32httpfirstrespondtime > 0 && u32httpfirstrespondtime < 4294967295L && u32httplastpackettime > 0 && u32httplastpackettime < 480000) {
          http_success_cnt = 1
          http_success_delay = u32httplastpackettime
        }
        if (u16HttpWapStat > 0 && u16HttpWapStat < 400 && u32httpfirstrespondtime > 0 && u32httpfirstrespondtime < 480000) {
          http_response_cnt = 1
          http_response_delay = u32httpfirstrespondtime
        }
        if (u32dltraffic > 0 || u32ultraffic > 0) {
          active_user = s8imsi
        }

        outVal.append(http_req_cnt)
        outVal.append("|")
        outVal.append(http_success_cnt)
        outVal.append("|")
        outVal.append(http_success_delay)
        outVal.append("|")
        outVal.append(http_response_cnt)
        outVal.append("|")
        outVal.append(http_response_delay)
        outVal.append("|")
        outVal.append(http_ul_flow)
        outVal.append("|")
        outVal.append(http_dl_flow)
        outVal.append("|")
        outVal.append(active_user)
      } catch {
        case e: Exception => {
          logger.error(s"xdr parse error $fileds", e.getMessage)
        }
      }
      //
      List((u16citycode+ "|" + u8rat+ "|" + s8apn+ "|" + u32tac8, outVal.toString()),(u16citycode+ "|" + u32tac8, outVal.toString()))
      //(u16citycode+ "|" + u8rat+ "|" + s8apn+ "|" + u32tac8, outVal.toString())
    })//.filter(_._4.equals("86961903")).filter(_._4.equals("86959303"))
      //.map(x => {
       // (x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4, x._5) //按照业务规则取相关数据(维度，指标)
        //List((x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4, x._5),(x._1, x._5))
      //})
      
    //
    val group_map = logData.flatMap((item => item)).groupBy(x => x._1)
    //val group_map = logData.groupBy(x => x._1)

    
    val reduce_data = group_map.reduceGroup {
      (in: Iterator[(String, String)], out: Collector[(String, String)]) =>
        var keyStr = "";
        var keyLable = ""
        val active_user = scala.collection.mutable.HashSet[String]()
        //var newValues = new ArrayBuffer[Long]() //可变长报错size=8
        var newValues = new Array[Long](8)//定长

        while (in.hasNext) {
          val next = in.next()
          keyStr = next._1
          var tmpValues = next._2.split("\\|")
          for (i <- 0 until tmpValues.length) {
            //i=7
            if (i == tmpValues.length - 1) {
              active_user.add(tmpValues(i));
            } else {
              newValues(i) = newValues(i) + tmpValues(i).toLong;
            }
          }
        }
        var builder = new StringBuilder()
        if(keyStr.split("\\|").length>2){
          keyLable = "tac8"
        }else{
          keyLable = "citycode"
        }        
        /*else if(keyStr.split("\\|")(0).equals("0")){
          keyLable = "citycode_0"
        }else{
          keyLable = "citycode_512"
        }
        * 
        */
        builder.append(keyStr).append("|") //维度

        for (i <- 0 until newValues.length) {
          if (i == newValues.length - 1) {
            builder.append(active_user.size)
          } else {
            builder.append(newValues(i)).append("|")
          }
        }

        out.collect((keyLable,builder.toString()))
    }    
    val outPath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\dataset\\out"
    //reduce_data.writeAsText(outPath, WriteMode.OVERWRITE)
    
    val multipleTextOutputFormat = new FlinkMultipleTextOutputFormat[String, String]()
    val jc = new JobConf()
    FileOutputFormat.setOutputPath(jc, new Path(outPath))
    val format = new HadoopOutputFormat[String, String](multipleTextOutputFormat, jc)    
    //reduce_data.setParallelism(1).output(format)
    reduce_data.output(format)
		
    //val outPath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\dataset\\out"
    //logData.writeAsText(outPath, WriteMode.OVERWRITE)
    env.execute("IOTAnalysis")   
  }
}