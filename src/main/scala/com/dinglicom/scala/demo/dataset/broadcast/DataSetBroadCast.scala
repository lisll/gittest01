package com.dinglicom.scala.demo.dataset.broadcast

import java.util
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import scala.collection.mutable

/**
 * 需求：求取订单对应的商品(将订单和商品数据进行合并成为一条数据)
 * 
 * 注意：数据格式参见附件中的orders.txt以及product.txt，商品表当中的第1个字段表示商品id，订单表当中的第3个字段表示商品id，字段之间都是使用","进行切割
 */
object DataSetBroadCast {
  
  def main(args: Array[String]): Unit = {
    
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    
    val productData: DataSet[String] = environment.readTextFile("file:///F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\product\\product.txt")
    val productMap = new mutable.HashMap[String,String]
    val prouctMapSet: DataSet[mutable.HashMap[String, String]] = productData.map(x => {
      val strings: Array[String] = x.split(",")
      println(strings(0))
      productMap.put(strings(0), x)
      productMap
    })
    
    //获取商品数据
    val ordersDataset: DataSet[String] = environment.readTextFile("file:///F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\product\\orders.txt")
    
    //将商品数据转换成为map结构，key为商品id，value为一行数据
    val resultLine: DataSet[String] = ordersDataset.map(new RichMapFunction[String, String] {
      var listData: util.List[Map[String, String]] = null
      var allMap = Map[String, String]()
    
      override def open(parameters: Configuration): Unit = {
        this.listData = getRuntimeContext.getBroadcastVariable[Map[String, String]]("productBroadCast")
        val listResult: util.Iterator[Map[String, String]] = listData.iterator()
        while (listResult.hasNext) {
          allMap =  allMap.++(listResult.next())
        }
      }
    
      //获取到了订单数据，将订单数据与商品数据进行拼接成为一整
      override def map(eachOrder: String): String = {
        val str: String = allMap.getOrElse(eachOrder.split(",")(2),"暂时没有值")
        eachOrder + "," + str
      }
    }).withBroadcastSet(prouctMapSet, "productBroadCast")
    resultLine.print()
    
    //environment.execute("broadCastJoin")    
  }
}