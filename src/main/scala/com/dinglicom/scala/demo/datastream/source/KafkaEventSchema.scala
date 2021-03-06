package com.dinglicom.scala.demo.datastream.source

import net.sf.json.JSONObject
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation

/**
  * <p/>
  * <li>Description: 自定义Json反序列化器</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020-02-15 11:30</li>
  */
class KafkaEventSchema extends DeserializationSchema[JSONObject] with SerializationSchema[JSONObject]{

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
