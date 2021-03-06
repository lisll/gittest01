package com.dinglicom.scala.demo.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
  * <p/>Table API
  * <li>Description: Batch Table</li>
  * <li>@author: wubo</li>
  * <li>Date: 2020/02/14 11:30</li>
  * 
  * 学习参考:https://www.jianshu.com/p/97eaade9fa08
  */
object BatchTableApiApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val filePath = "F:\\DL_ZH_SVN\\trunk\\cloudil-p3\\flink\\flink-local-train\\src\\main\\resources\\data\\table\\sales.csv"

    import org.apache.flink.api.scala._

    // 已经拿到DataSet
    val csv = env.readCsvFile[SalesLog](filePath,ignoreFirstLine=true)

    // DataSet ==> Table
    val salesTable = tableEnv.fromDataSet(csv)

    // Table ==> table
    tableEnv.registerTable("sales", salesTable)

    // sql
    val resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId")

    tableEnv.toDataSet[Row](resultTable).print()

  }

  case class SalesLog(transactionId:String,
                      customerId:String,
                      itemId:String,
                      amountPaid:Double)
}
