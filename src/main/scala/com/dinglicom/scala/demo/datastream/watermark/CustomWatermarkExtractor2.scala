package com.dinglicom.scala.demo.datastream.watermark

import net.sf.json.JSONObject
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import java.text.SimpleDateFormat

/**
  * <p/>watermarks的生成方式有两种
  * 1：With Periodic Watermarks：周期性的触发watermark的生成和发送（比较常用）
  * 2：With Punctuated Watermarks：基于某些事件触发watermark的生成和发送
  * 
  * <li>Description: 自定义watermark发射器</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-18 11:00</li>
  */
class CustomWatermarkExtractor2 extends AssignerWithPeriodicWatermarks[JSONObject] {
  
  var currentMaxTimestamp = 0L;
  
  /*
   * 这个要结合自己的业务以及数据情况去设置。
   * 如果maxOutOfOrderness设置的太小，而自身数据发送时由于网络等原因导致乱序或者late太多，
   * 那么最终的结果就是会有很多单条的数据在window中被触发，数据的正确性影响太大 
   * 对于严重乱序的数据，需要严格统计数据最大延迟时间，才能保证计算的数据准确，
   * 延时设置太小会影响数据准确性，延时设置太大不仅影响数据的实时性，更加会加重Flink作业的负担，
   * 不是对eventTime要求特别严格的数据，尽量不要采用eventTime方式来处理，会有丢数据的风险
   */
  val maxOutOfOrderness = 10000L;// 最大允许的乱序时间是10s
            
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  /**
    * 获取当前水位线
    * 
    * 默认100ms被调用一次
    * @return
    */
  override def getCurrentWatermark: Watermark = {
      new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }

  /**
    * 抽取时间戳
    *
    * @param element
    * @param previousElementTimestamp
    * @return
    */
  override def extractTimestamp(element: JSONObject, previousElementTimestamp: Long): Long = {

    val timestamp = element.getLong("time") * 1000
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    
    System.out.println("key:"+element.getString("fruit")+",eventtime:["+element.getLong("time")+"|"+sdf.format(timestamp)+"],currentMaxTimestamp:["+currentMaxTimestamp+"|"+sdf.format(currentMaxTimestamp)+"],watermark:["+getCurrentWatermark().getTimestamp()+"|"+sdf.format(getCurrentWatermark().getTimestamp())+"]")
    
    timestamp
  }
}

/*
 * 
 * 


{fruit:"apple",idx:1,number:1,time:1538359882000}			2018-10-01 10:11:22.000

{fruit:"apple",idx:2,number:2,time:1538359883000}			2018-10-01 10:11:23.000

{fruit:"apple",idx:3,number:1,time:1538359886000}			2018-10-01 10:11:26.000

{fruit:"apple",idx:4,number:1,time:1538359892000}			2018-10-01 10:11:32.000

{fruit:"apple",idx:5,number:1,time:1538359893000}			2018-10-01 10:11:33.000

{fruit:"apple",idx:6,number:1,time:1538359894000}			2018-10-01 10:11:34.000		[10:11:21.000		10:11:24.000)

window的触发机制，是先按照自然时间将window划分，如果window大小是3秒，那么1分钟内会把window划分为如下的形式【左闭右开】:
—————————————————————
[00:00:00,00:00:03)
[00:00:03,00:00:06)
[00:00:06,00:00:09)
[00:00:09,00:00:12)
[00:00:12,00:00:15)
[00:00:15,00:00:18)
[00:00:18,00:00:21)
[00:00:21,00:00:24)
[00:00:24,00:00:27)
[00:00:27,00:00:30)
[00:00:30,00:00:33)
[00:00:33,00:00:36)
[00:00:36,00:00:39)
[00:00:39,00:00:42)
[00:00:42,00:00:45)
[00:00:45,00:00:48)
[00:00:48,00:00:51)
[00:00:51,00:00:54)
[00:00:54,00:00:57)
[00:00:57,00:01:00)
—————————————————————
window的设定无关数据本身，而是系统定义好了的
输入的数据中，根据自身的Event Time，将数据划分到不同的window中，如果window中有数据，
则当watermark时间>=Event Time时，就符合了window触发的条件了，
最终决定window触发，还是由数据本身的Event Time所属的window中的window_end_time决定
—————————————————————
{fruit:"apple",idx:1,number:1,time:1538359881}		2018-10-01 10:11:21.000
{fruit:"apple",idx:1,number:1,time:1538359882}		2018-10-01 10:11:22.000
{fruit:"apple",idx:2,number:2,time:1538359883}		2018-10-01 10:11:23.000

{fruit:"apple",idx:3,number:3,time:1538359884}		2018-10-01 10:11:24.000
{fruit:"apple",idx:4,number:2,time:1538359885}		2018-10-01 10:11:25.000
{fruit:"apple",idx:5,number:6,time:1538359886}		2018-10-01 10:11:26.000		[10:11:21.000		10:11:24.000) total:3

{fruit:"apple",idx:6,number:6,time:1538359887}		2018-10-01 10:11:27.000

{fruit:"apple",idx:7,number:1,time:1538359892}		2018-10-01 10:11:32.000

{fruit:"apple",idx:8,number:1,time:1538359893}		2018-10-01 10:11:33.000

{fruit:"apple",idx:9,number:1,time:1538359894}		2018-10-01 10:11:34.000		[10:11:21.000		10:11:24.000) total:4

{fruit:"apple",idx:10,number:1,time:1538359896}		2018-10-01 10:11:36.000

{fruit:"apple",idx:11,number:1,time:1538359897}		2018-10-01 10:11:37.000		[10:11:24.000		10:11:27.000) total:11

*/