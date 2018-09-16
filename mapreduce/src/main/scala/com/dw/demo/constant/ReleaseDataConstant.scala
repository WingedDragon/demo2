package com.dw.demo.constant

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
  * Created by finup on 2018/5/16.
  */
object ReleaseDataConstant {

  //range
  val RANGE_DAYS = 2

  //config
  val CONFIG_JDBC_PATH = "jdbc.properties"



  //pv状态
  val STATUS_PV :String = "pv"
  val STATUS_UV :String = "uv"

  //竞价单次成本-> 元
  val BIDDING_COSTPRICE_FACTOR = 100000


  //partition
  val DEF_PARTITIONS_FACTOR = 4
  val DEF_FILEPARTITIONS_FACTOR = 10
  val DEF_SOURCE_PARTITIONS = 16
  val DEF_OTHER_PARTITIONS = 8
  val DEF_STORAGE_LEVEL :StorageLevel= StorageLevel.MEMORY_AND_DISK

  val DEF_SAVEMODE:SaveMode  = SaveMode.Append



  //table
  //投放请求日志
  val ODS_RELEASE_SESSIONS :String = "ods.release_sessions"

  //投放非目标客户表
  val DW_RELEASE_NOTCUSTOMER :String = "dw.release_notcustomer"

  //投放目标客户表
  val DW_RELEASE_CUSTOMER :String = "dw.release_customer"

  //投放竞价表
  val DW_RELEASE_BIDDING :String = "dw.release_bidding"

  //投放曝光表
  val DW_RELEASE_SHOW :String = "dw.release_show"

  //投放点击表
  val DW_RELEASE_CLICK :String = "dw.release_click"




}
