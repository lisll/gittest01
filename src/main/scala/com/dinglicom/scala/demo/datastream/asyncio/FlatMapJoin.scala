package com.dinglicom.scala.demo.datastream.asyncio

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.dinglicom.scala.demo.datastream.source.KafkaEventSchema
import com.dinglicom.scala.demo.utils.ConfigUtils
import net.sf.json.JSONObject
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * <p/>
  * <li>Description: mapFunction算子同步访问外部存储</li>
  *
  * 弊端：
  * 和数据库交互过程是一个同步，后一个会等待前一个完成。
  * 注意：
  * 可以通过增加MapFunction的一个较大并行度也可以改善吞吐量的，但是这意味着更高的资源开销，
  * 更多的MapFunction实例意味着更多的task，线程，Flink内部网络连接，数据库的链接，缓存，更多内部状态开销
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-21 14:30</li>
  */
object FlatMapJoin {
  def main(args: Array[String]): Unit = {

    //构建运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置最少一次和恰一次处理语义
    env.enableCheckpointing(20000, CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint目录
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /**
      * 设置重启策略/5次尝试/每次尝试间隔50s
      */
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000))
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //json:{fruit:"fruit_1",name:"apple",number:10}
    //json:{fruit:"fruit_2",name:"pear",number:20}
    //json:{fruit:"fruit_3",name:"orange",number:30}    
    val kafkaConfig = ConfigUtils.apply("string") 

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,
      new KafkaEventSchema(),
      kafkaConfig._2)
      .setStartFromLatest()

    import org.apache.flink.api.scala._
    val source: DataStream[JSONObject] = env.addSource(kafkaConsumer)
      .keyBy(_.getString("fruit"))
      .flatMap(new JoinWithRedis)

    source.print()
    env.execute("FlatmapJoin")

  }

  class JoinWithRedis extends RichFlatMapFunction[JSONObject, JSONObject] {

    var jeDis: Jedis = _
    var cache: Cache[String, String] = _

    /**
      * 在执行flatMap之前进行相关变量初始化
      *
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
      jeDis = new Jedis("localhost", 6379)
      cache = Caffeine
        .newBuilder
        .maximumSize(1025)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .build[String, String]
    }

    override def close(): Unit = {
      if (jeDis != null && jeDis.isConnected)
        jeDis.close()
      if (cache != null)
        cache.cleanUp()
    }

    override def flatMap(value: JSONObject, out: Collector[JSONObject]): Unit = {

      if (jeDis != null && (!jeDis.isConnected)) {
        val fruit = value.getString("fruit")
        val cache_data = cache.getIfPresent(fruit)
        System.out.println("cache_data:" + cache_data)
        if (null != cache_data) {
          value.put("docs", cache_data)
          out.collect(value)
        }
        else {
          val s = jeDis.get(fruit)
          if (s != null) {
            cache.put(fruit, s)
            value.put("docs", s)
          } else {
            jeDis.set(fruit, value.toString())
            cache.put(fruit, value.toString())
          }
          out.collect(value)
        }
      }
    }
  }

}
