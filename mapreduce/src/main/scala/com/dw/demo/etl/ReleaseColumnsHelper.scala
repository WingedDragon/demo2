package com.dw.demo.etl

import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by finup on 2018/5/12.
  */
object ReleaseColumnsHelper {


  val ORDER_STATUS_050 :String = "050"
  val ORDER_STATUS_051 :String = "051"

  //表
  var tableColumns :mutable.Map[String,Seq[String]] = mutable.Map[String,Seq[String]]()

  //状态
  var statusColumns :mutable.Map[String,Seq[String]] = mutable.Map[String,Seq[String]]()

  //可重复sessionId
  var statusDupleColumns :mutable.Map[String,Seq[String]] = mutable.Map[String,Seq[String]]()




  /**
    * 基本列选取
    * @return
    */
  def convertColumns(df:DataFrame, cols:Seq[String]):ArrayBuffer[Column] ={
    val columns = new ArrayBuffer[Column]()
    for(col <- cols){
      columns.+=(df(col))
    }
    columns
  }

  /**
    * 基本列选取
    * @return
    */
  def selectReleaseRequestLogsColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("reqid")
    columns.+=("sessionid")
    columns.+=("device")
    columns.+=("source")
    columns.+=("status")
    columns.+=("ct")
    columns.+=("exts['code'] as code")
    columns.+=("exts['bid'] as bid")
    columns.+=("exts['price'] as price")
    columns.+=("bdp_day")
    //columns.+=("from_unixtime(ct/1000,'yyyyMMdd') as bdp_day")
    columns
  }


  /**
    * 投放目标客户列选取
    * @return
    */
  def selectReleaseCustomerColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("reqid")
    columns.+=("sessionid")
    columns.+=("device")
    columns.+=("source")
    columns.+=("ct")
    columns.+=("code")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 投放非目标客户列选取
    * @return
    */
  def selectReleaseNotCustomerColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("reqid")
    columns.+=("sessionid")
    columns.+=("device")
    columns.+=("source")
    columns.+=("ct")
    columns.+=("bid")
    columns.+=("code")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 投放竞价列选取
    * @return
    */
  def selectRealeaseBiddingColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("reqid")
    columns.+=("sessionid")
    columns.+=("device")
    columns.+=("source")
    columns.+=("ct")
    columns.+=("price")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 投放曝光列选取
    * @return
    */
  def selectRealeaseShowColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("reqid")
    columns.+=("sessionid")
    columns.+=("device")
    columns.+=("source")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 投放曝光列选取
    * @return
    */
  def selectRealeaseClickColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("reqid")
    columns.+=("sessionid")
    columns.+=("device")
    columns.+=("source")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 通用列选取
    * @return
    */
  def selectRelease4UVColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("source")
    columns.+=("device")
    columns.+=("bdp_date")
    columns
  }



  /**
    * mysql 报表
    * @return
    */
  def selectMyqlReportColumns():ArrayBuffer[String] ={
    val ncolumns = new ArrayBuffer[String]()
    ncolumns.+=("source")
    ncolumns.+=("device")
    ncolumns.+=("bdp_date")
    ncolumns
  }





}
