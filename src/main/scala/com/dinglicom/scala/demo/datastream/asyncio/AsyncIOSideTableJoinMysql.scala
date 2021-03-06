package com.dinglicom.scala.demo.datastream.asyncio

import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.dinglicom.scala.demo.utils.ConfigUtils
import io.vertx.core.json.JsonObject
import io.vertx.core.{AsyncResult, Handler, Vertx, VertxOptions}
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.{ResultSet, SQLConnection}
import net.sf.json.JSONObject
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, DataStream}
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * <p/>
  * <li>Description: 异步IO写入mysql数据库</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-22 13:42</li>
  * 异步IO三步曲：
  *     1.AsyncFunction的一个实现，用来分派请求
  *     2.获取操作结果并将其传递给ResultFuture的回调
  *     3.将异步I/O操作作为转换应用于DataStream
  * 注意：
  * vertx目前只支持scala 2.12的版本,该demo不能编译通过，请参考java版本
  * <dependency>
  * <groupId>io.vertx</groupId>
  * <artifactId>vertx-lang-scala_2.12</artifactId>
  * <version>3.5.4</version>
  * </dependency>
  *
  * 学习参考：https://blog.csdn.net/qq_37142346/article/details/89813535
  */
object AsyncIOSideTableJoinMysql {
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
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    
    //json:{fruit:"fruit_1",name:"apple",number:10}
    //json:{fruit:"fruit_2",name:"pear",number:20}
    //json:{fruit:"fruit_3",name:"orange",number:30}    
    val kafkaConfig = ConfigUtils.apply("json")

    val kafkaConsumer = new FlinkKafkaConsumer(kafkaConfig._1,
      new JSONEventSchema, //自定义反序列化器
      kafkaConfig._2)


    //注意DataStream的包 org.apache.flink.streaming.api.datastream
    val source: DataStream[JSONObject] = env.addSource(kafkaConsumer)

    //AsyncDataStream.orderedWait();（有序）：消息的发送顺序与接收到的顺序相同（包括 watermark ），也就是先进先出。
    //AsyncDataStream.unorderWait(); （无序）：
    //1.在ProcessingTime中，完全无序，即哪个请求先返回结果就先发送（最低延迟和最低消耗）。
    //2.在EventTime中，以watermark为边界，介于两个watermark之间的消息可以乱序，但是watermark和消息之间不能乱序，
    //  这样既认为在无序中又引入了有序，这样就有了与有序一样的开销。（具体我们会在后面的原理中讲解）
    
    //进行异步IO操作
    val result = if (true) {
      AsyncDataStream.orderedWait(source,
        new SampleAsyncFunction,
        1000000L, //异步请求超时时间
        TimeUnit.MILLISECONDS,
        20) //异步请求容量
        .setParallelism(1)
    } else {
      AsyncDataStream.unorderedWait(source,
        new SampleAsyncFunction,
        1000000L, //异步请求超时时间
        TimeUnit.MILLISECONDS,
        20) //异步请求容量
        .setParallelism(1)
    }

    result.print()

    env.execute("AsyncIoSideTableJoinMysqlJava")

  }
  /**
        说明：
        1、AsyncDataStream有2个方法，
        	unorderedWait表示数据不需要关注顺序，处理完立即发送，
        	orderedWait表示数据需要关注顺序，为了实现该目标，操作算子会在该结果记录之前的记录为发送之前缓存该记录。
        	这往往会引入额外的延迟和一些Checkpoint负载，因为相比于无序模式结果记录会保存在Checkpoint状态内部较长的时间。
        2、Timeout配置，主要是为了处理死掉或者失败的任务，防止资源被长期阻塞占用。
        3、最后一个参数Capacity表示同时最多有多少个异步请求在处理，异步IO的方式会导致更高的吞吐量，但是对于实时应用来说该操作也是一个瓶颈。限制并发请求数，算子不会积压过多的未处理请求，但是一旦超过容量的显示会触发背压。
        该参数可以不配置，但是默认是100
    * AsyncFunction，该函数实现了异步请求分发的功能
    */
  
  class SampleAsyncFunction extends RichAsyncFunction[JSONObject, JSONObject] {

    //mySQLClient就是在可序列化对象里某些成员在序列化时不被序列化
    @transient var mySQLClient: JDBCClient = _
    var cache: Cache[String, String] = _

    /**
      * 函数的初始化方法。在实际工作方法之前调用它
      *
      * @param parameters
      */
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 初始化本地缓存
      cache = Caffeine
        .newBuilder()
        .maximumSize(1025)
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .build[String, String]

      // Mysql客户端配置信息
      val mysqlConfig = new JsonObject()
        .put("url", "jdbc:mysql://localhost:3306/flink_demo")
        .put("driver_class", "com.mysql.jdbc.Driver")
        .put("max_pool_size", 20)
        .put("user", "root")
        .put("password", "root")
        .put("max_idle_time", 1000)

      val vo = new VertxOptions()
      vo.setEventLoopPoolSize(10)
      vo.setWorkerPoolSize(20)

      val vt = Vertx.vertx(vo)
      // 创建Mysql客户端
      mySQLClient = JDBCClient.createNonShared(vt, mysqlConfig)

    }

    /**
      * 用户代码的拆除方法。在最后一次调用。对于作为迭代一部分的函数，将在每次迭代步骤后调用此方法。
      */
    override def close(): Unit = {
      super.close()
      if (mySQLClient != null) {
        mySQLClient.close()
      }
      if (cache != null) {
        cache.cleanUp()
      }
    }

    /**
      * 触发每个流输入的异步操作
      *
      * @param input
      * @param resultFuture
      */
    override def asyncInvoke(input: JSONObject, resultFuture: ResultFuture[JSONObject]): Unit = {

      val key = input.getString("fruit")
      //在缓存中判断key是否存在
      val cacheIfExist = cache.getIfPresent(key)
      if (cacheIfExist != null) {
        input.put("docs", cacheIfExist)
        resultFuture.complete(Collections.singleton(input))
        return
      }
      mySQLClient.getConnection(new Handler[AsyncResult[SQLConnection]] {
        override def handle(event: AsyncResult[SQLConnection]): Unit = {
          if (event.failed()) {
            resultFuture.complete(null)
            return
          }
          val conn = event.result()
          //结合业务拼sql
          val querySql = "SELECT docs FROM testJoin where fruit = '" + key + "'"

          conn.query(querySql, new Handler[AsyncResult[ResultSet]] {
            override def handle(event: AsyncResult[ResultSet]): Unit = {
              if (event.failed()) {
                resultFuture.complete(null)
                return
              }

              if (event.succeeded()) { //成功情况下
                val rs = event.result()

                val rows: util.List[JsonObject] = rs.getRows
                if (rows.size() <= 0) {
                  resultFuture.complete(null)
                  return
                }

                import scala.collection.JavaConverters._
                rows.asScala.foreach(e => {
                  val desc = e.getString("docs")
                  input.put("docs", desc)
                  cache.put(key, desc)
                  resultFuture.complete(Collections.singleton(input))
                })

              } else { //异常的情况，不进行处理会阻塞
                resultFuture.complete(null)
              }

              conn.close(new Handler[AsyncResult[Void]] {
                override def handle(event: AsyncResult[Void]): Unit = {
                  if (event.failed()) throw new RuntimeException(event.cause)
                }
              })
            }
          })
        }
      })

    }
  }

  class JSONEventSchema extends DeserializationSchema[JSONObject] with SerializationSchema[JSONObject] {

    override def deserialize(message: Array[Byte]): JSONObject = {
      JSONObject.fromObject(new String(message))
    }

    override def isEndOfStream(nextElement: JSONObject): Boolean = false

    override def serialize(element: JSONObject): Array[Byte] = {
      element.toString().getBytes()
    }

    override def getProducedType: TypeInformation[JSONObject] = {

      TypeInformation.of(classOf[JSONObject])
    }
  }

}
