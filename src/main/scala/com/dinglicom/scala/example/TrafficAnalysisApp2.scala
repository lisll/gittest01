package com.dinglicom.scala.example

import java.util.{Date, Properties}
import java.text.SimpleDateFormat

import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.util.Collector
import scala.collection.mutable.ArrayBuffer

import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer
import java.time.ZoneId
import org.apache.flink.streaming.api.scala.OutputTag
import scala.collection.mutable.ListBuffer
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.commons.io.FileUtils


/**
 * 流量分析系统
 * 功能： 
 * 		最近一分钟每个域名产生的流量统计
 * 		Flink接受Kafka数据处理+Flink读取Hdfs用户号码的配置数据
 * 
 * 日期：2020-03-19 10:00
 * 1.正常输出到HDFS
 */
object TrafficAnalysisApp2 {

  val logger = LoggerFactory.getLogger("TrafficAnalysis2")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    //设置事件时间作为flink处理的基准时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.api.scala._
    
    //本地地址OK
    var cfgfilePath = "/home/cloudil/wubo/flink_learn/tac8_cfg.txt"
    
    //hdfs注册失败-原因不明
    //val cfgfilePath = "hdfs://nameservice1/tmp/flink_cfg/tac8_cfg.txt"
    
    // step1: 注册一个本地/HDFS文件
    env.registerCachedFile(cfgfilePath, "tac8_cfg", true)

    val topic = "http514"
    // kafka params
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.41.100:9092,172.16.41.178:9092")
    properties.setProperty("group.id", "rd_http")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("sasl.kerberos.service.name", "kafka")
    properties.setProperty("security.protocol", "SASL_PLAINTEXT")

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
      //.setStartFromLatest()
    
    val lateOutputTag = new OutputTag[(Long, String, Long)]("late-data")
    
    val value: DataStream[String] = env.addSource(consumer)
    //1. 接受来自kafka的数据,配置数据源
    val data = value
    //2.数据清洗
    val logData = data.map(new RichMapFunction[String, (String, String, Long, String, Long)] {
      var allMap = scala.collection.mutable.Map[String, Int]()
      var list1 = new ListBuffer[String]
      // step2：在open方法中获取到分布式缓存的内容即可
      override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache().getFile("tac8_cfg")

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
      
      override def map(x: String): (String, String, Long, String, Long) = {
        val splits = x.split("\\|")
        val citycode = splits(Ltec1_http_cs.u16citycode)
        var tac8 = ""
        if(list1.contains(splits(Ltec1_http_cs.u32tac8))){
          tac8 = splits(Ltec1_http_cs.u32tac8)
        }          
        val timeStr = splits(Ltec1_http_cs.u32begintime).toLong
        var time = 0L
        //时间处理
        try {
          //val sourceFormat = new SimpleDateFormat("yyy-MM-dd HH:mm:ss")
          //time = sourceFormat.parse(timeStr).getTime
          time = timeStr * 1000
        } catch {
          case e: Exception => {
            logger.error(s"time parse error $timeStr", e.getMessage)
          }
        }
        val host = splits(Ltec1_http_cs.s8host)
        val traffic = splits(Ltec1_http_cs.u32ultraffic).toLong
        (citycode, tac8, time, host, traffic)
      }
    }).filter(_._1 != 0).filter(_._2.nonEmpty).filter(_._4.contains("weixin.qq.com")) //过滤掉citycode非0和host包含"weixin.qq.com"的数据
      .map(x => {
        (x._3, x._4, x._5) //数据清洗按照业务规则取相关数据 1citycode(不需要可以抛弃) 2time 3host 4traffic
    })
    
    //3. 设置timestamp和watermark,解决时序性问题
    // AssignerWithPeriodicWatermarks[T] 对应logdata的tuple类型
    val resultData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {
      //最大无序容忍的时间 10s
      val maxOutOfOrderness = 30000L
      //当前最大的TimeStamp
      var currentMaxTimeStamp: Long = _
      /**
       * 设置TimeStamp生成WaterMark
       */
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimeStamp - maxOutOfOrderness)
      }
      /**
       * 抽取时间
       */
      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        //获取数据的event time
        val timestamp = element._1
        currentMaxTimeStamp = Math.max(timestamp, currentMaxTimeStamp)
        timestamp
      }
    })//4. 根据window进行业务逻辑的处理   最近一分钟每个域名产生的流量
      .keyBy(1) // 此处按照域名进行keyBy
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .allowedLateness(Time.seconds(10)) //允许10s延迟
      //.sideOutputLateData(lateOutputTag) //延迟数据侧输出      
      .apply(new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {

          val domain = key.getField(0).toString
          var sum = 0L
          val times = ArrayBuffer[Long]()

          val iterator = input.iterator
          while (iterator.hasNext) {
            val next = iterator.next()
            sum += next._3         // traffic流量字段求和
            times.append(next._1)
          }

          /** 第一个参数：这一分钟的时间 2020-02-29 16:20 第二个参数：域名 第三个参数：traffic的和 */
          val time = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(times.max))
          out.collect((time, domain, sum))//out: Collector[(String, String, Long)]
          //out.collect(time+"|"+domain+"|"+sum) //out: Collector[String]
        }
      })    

		//val hadoopSink = new BucketingSink[String]("hdfs://nameservice1/tmp/test/flink_out61");
		//hadoopSink.setBucketer(new DateTimeBucketer[String]("yyyyMMddHH", ZoneId.of("Asia/Shanghai")));      
    
    val hadoopSink = new BucketingSink[(String, String, Long)]("hdfs://nameservice1/tmp/test/flink_out61");
    hadoopSink.setBucketer(new DateTimeBucketer[(String, String, Long)]("yyyyMMddHH", ZoneId.of("Asia/Shanghai")));
		// 使用东八区时间格式"yyyyMMddHH"命名存储区
		// 下述两种条件满足其一时，创建新的块文件
		// 条件1.设置块大小为50MB
		//hadoopSink.setBatchSize(1024 * 1024 * 10);
		// 条件2.设置时间间隔1min
		hadoopSink.setBatchRolloverInterval(1 * 60 * 1000);
		//_part-0-0.pending|_part-0-5.in-progress
		
		// 设置块文件前缀
		hadoopSink.setPendingPrefix("");
		// 设置块文件后缀
		hadoopSink.setPendingSuffix("");
		// 设置运行中的文件前缀
		hadoopSink.setInProgressPrefix(".");
		//part-0-0|part-0-1|.part-0-2.in-progress
		
		resultData.addSink(hadoopSink)
		
    env.execute("TrafficAnalysis")      
  }
}