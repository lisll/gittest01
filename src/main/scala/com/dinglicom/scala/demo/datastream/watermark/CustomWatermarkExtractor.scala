package com.dinglicom.scala.demo.datastream.watermark

import net.sf.json.JSONObject
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import java.text.SimpleDateFormat

/**
  * <p/>
  * <li>Description: 自定义watermark发射器</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-16 10:37</li>
  */
class CustomWatermarkExtractor extends AssignerWithPeriodicWatermarks[JSONObject] {

  var currentTimestamp = Long.MinValue

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  
  /**
    * waterMark生成器
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
  override def extractTimestamp(element: JSONObject, previousElementTimestamp: Long): Long = {

    this.currentTimestamp = element.getLong("time") * 1000
    System.out.println("key:"+element.getString("fruit")+",eventtime:["+element.getLong("time")+"|"+sdf.format(element.getLong("time"))+"],watermark:["+getCurrentWatermark().getTimestamp()+"|"+sdf.format(getCurrentWatermark().getTimestamp())+"]")
    currentTimestamp
  }
}
