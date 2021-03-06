package com.dinglicom.scala.demo.datastream.join

import com.dinglicom.scala.demo.utils.ConfigUtils
import org.apache.flink.api.common.functions.{JoinFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.text.SimpleDateFormat

/**
  * Description: window之间的join操作
  * author: wubo
  * Date: 2020-02-25 15:52
  * 注意：测试的时候将kafkaProduce中类kv数据生成方法 join情况打开
  */
object SessionWindowJoin {
  def main(args: Array[String]): Unit = {

    //构建运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置最少一次和恰一次处理语义
    env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint目录
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, //5次尝试
      50000)) //每次尝试间隔50s
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val kafkaConfig = ConfigUtils.apply("kv")
    val kafkaConfig1 = ConfigUtils.apply("kv1")

    //构建kafka消费者
    val kafkaConsumer1 = new FlinkKafkaConsumer(kafkaConfig._1, new SimpleStringSchema(), kafkaConfig._2)
      .setStartFromLatest()
      .assignTimestampsAndWatermarks(new CustomWatermarkExtractor) //设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置


    val kafkaConsumer2 = new FlinkKafkaConsumer(kafkaConfig1._1, new SimpleStringSchema(), kafkaConfig1._2)
      .setStartFromLatest()
      .assignTimestampsAndWatermarks(new CustomWatermarkExtractor)


    import org.apache.flink.api.scala._
    val operator1 = env.addSource(kafkaConsumer1)
      .map(new RichMapFunction[String, (String, Long)] {
        override def map(value: String): (String, Long) = {
          val splits = value.split(",")
          (splits(0), splits(1).toLong)
        }
      })

    val operator2 = env.addSource(kafkaConsumer2)
      .map(new RichMapFunction[String, (String, Long)] {
        override def map(value: String): (String, Long) = {
          val splits = value.split(",")
          (splits(0), splits(1).toLong)
        }
      })

    operator1
      .join(operator2)
      .where(elem => elem._1)
      .equalTo(elem => elem._1) //注意单位  Time.minutes(1)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3))) //窗口分配器定义程序
      .apply(new JoinFunction[(String, Long), (String, Long), (String, Long, Long)] {
        override def join(first: (String, Long), second: (String, Long)): (String, Long, Long) = {
          (first._1, first._2, second._2)
        }
      })
      .keyBy(_._1)
      .reduce((e1, e2) => {
        (e1._1, e1._2 + e2._2, e1._3 + e2._3) //聚合操作
      }).print()


    env.execute("SessionWindowJoin")

  }

  class CustomWatermarkExtractor extends AssignerWithPeriodicWatermarks[String] {

    var currentTimestamp = Long.MinValue
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    /**
      * waterMark生成器
      *
      * @return
      */
    override def getCurrentWatermark: Watermark = {

      new Watermark(
        if (currentTimestamp == Long.MinValue)
          Long.MinValue
        else
          (currentTimestamp - 10000)
      )
    }

    /**
      * 时间抽取
      *
      * @param element
      * @param previousElementTimestamp
      * @return
      */
    override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {

      val strings = element.split(",")
      this.currentTimestamp = strings(2).toLong
      System.out.println("key:"+strings(0)+",eventtime:["+strings(2)+"|"+sdf.format(strings(2).toLong)+"],watermark:["+getCurrentWatermark().getTimestamp()+"|"+sdf.format(getCurrentWatermark().getTimestamp())+"]")
      currentTimestamp
    }
  }


}
/**


kv
0001,1,1538359882000
0001,2,1538359886000
0001,3,1538359892000
0001,4,1538359893000
0001,5,1538359894000

kv1
0001,11,1538359882000
0001,12,1538359886000
0001,13,1538359892000
0001,14,1538359893000
0001,15,1538359894000


*/