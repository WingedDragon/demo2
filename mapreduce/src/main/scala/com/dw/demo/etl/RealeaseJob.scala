package com.dw.demo.etl

import com.dw.demo.constant.ReleaseDataConstant
import com.dw.demo.hdfs.{HdfsUtil, RegexExcludePathFilter}
import com.dw.demo.util.{ReleaseHelper, SparkHelper}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, SparkConf}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by finup on 2018/7/18.
  */

class RealeaseJob{

}

object RealeaseJob {


  val logger :Logger = LoggerFactory.getLogger(RealeaseJob.getClass)




  /**
    * 中间数据信息处理
    */
  def mapDataSaveTable(df :DataFrame, columns:Seq[String], table:String, mode:SaveMode) :Unit ={
    val begin = System.currentTimeMillis()
    val ndf :DataFrame = df.selectExpr(columns:_*)
    //val redf = repartitionsDF(ndf)

    //写入表数据
    ndf.write.mode(mode).insertInto(table)
    println(s"df[${table}] End use:${System.currentTimeMillis() - begin}=========================>")
  }


  /**
    * 数据源处理
    */
  def sourceDataHandle(spark:SparkSession, tableName:String, bdp_day:String, storageLevel:StorageLevel) :DataFrame ={
    val begin = System.currentTimeMillis()
    //1 投放原始数据
    val hql = s"""
          select
              *
          	from ${tableName}
          	where
          		bdp_day = '${bdp_day}'
        """.stripMargin
    //println(s"sourceDataHandle.hql===>$hql")
    val columns = ReleaseColumnsHelper.selectReleaseRequestLogsColumns()
    val sourceDF = spark.sql(hql).selectExpr(columns:_*)
      .persist(storageLevel)
    sourceDF
  }


  /**
    * 中间数据处理并保存
    */
  def statusDataSaveTable(sourceDF :DataFrame, status:String, table:String, columns:Seq[String], mode:SaveMode) :Unit ={
    val begin = System.currentTimeMillis()
    val df = sourceDF.filter(sourceDF("status") === status)
      .selectExpr(columns:_*)
      .distinct()
    //val ndf = repartitionsDF(df)

    //写入表数据
    df.write.mode(mode).insertInto(table)
    println(s"df[${status}] use:${System.currentTimeMillis() - begin}=========================>")
  }


  /**
    * 投放相关数据处理
    */
  def handleReleaseDatas(spark :SparkSession, bdp_day:String, storageLevel:StorageLevel) :Unit = {
    try{

      //投放原始数据(当天)
      val releaseDF = sourceDataHandle(spark, ReleaseDataConstant.ODS_RELEASE_SESSIONS, bdp_day, storageLevel)


      //目标客户
      statusDataSaveTable(releaseDF, ReleaseHelper.RELEASE_CUSTOMER, ReleaseDataConstant.DW_RELEASE_CUSTOMER, ReleaseColumnsHelper.selectReleaseCustomerColumns(),ReleaseDataConstant.DEF_SAVEMODE)

      //非目标客户
      statusDataSaveTable(releaseDF, ReleaseHelper.RELEASE_NOTCUSTOMER, ReleaseDataConstant.DW_RELEASE_NOTCUSTOMER, ReleaseColumnsHelper.selectReleaseNotCustomerColumns(),ReleaseDataConstant.DEF_SAVEMODE)

      //竞价
      statusDataSaveTable(releaseDF, ReleaseHelper.RELEASE_BIDDING, ReleaseDataConstant.DW_RELEASE_BIDDING, ReleaseColumnsHelper.selectRealeaseBiddingColumns(),ReleaseDataConstant.DEF_SAVEMODE)

      //曝光
      statusDataSaveTable(releaseDF, ReleaseHelper.RELEASE_SHOW, ReleaseDataConstant.DW_RELEASE_SHOW, ReleaseColumnsHelper.selectRealeaseShowColumns(),ReleaseDataConstant.DEF_SAVEMODE)

      //点击
      statusDataSaveTable(releaseDF, ReleaseHelper.RELEASE_CLICK, ReleaseDataConstant.DW_RELEASE_CLICK, ReleaseColumnsHelper.selectRealeaseClickColumns(), ReleaseDataConstant.DEF_SAVEMODE)

      //清除缓存
      releaseDF.unpersist()

    }catch{
      case ex:Exception => {
        println(s"$ex")
        logger.error(ex.getMessage, ex)
      }
    }
  }



  /**
    * 投放请求日志处理
    * @param spark
    * @param bdp_day
    */
  def handleLogDatas(spark :SparkSession, bdp_day:String, path:String, table:String, mode:SaveMode) :Unit = {
    var fs :FileSystem = null
    try{
      //hdfs FileSystem
      val fs :FileSystem = HdfsUtil.getFileSystem()
      val hdfsPath = new Path(path)
      if(fs.exists(hdfsPath)){
        val pathRegex = "xxx_*" //ad_detail_for_xxx
        val logRegexPathFilter = new RegexExcludePathFilter(pathRegex)
        val hdfsStatuss :Array[FileStatus] = fs.listStatus(hdfsPath, logRegexPathFilter)

        for(hdfsStatus <- hdfsStatuss){
          val subPath = hdfsStatus.getPath.toString

          //val parent = SparkHelper.parentPath(path)
          val realPath = subPath + "/bdp_day="+bdp_day
          val realPaths = ArrayBuffer[String](subPath)
          //println(s"subPath=${subPath},realPath=${realPath},realPaths=${realPaths}")

          val options = Map[String,String]("basePath" -> subPath)
          val columns = ReleaseColumnsHelper.selectReleaseRequestLogsColumns()
          val logDF = spark.read.options(options).parquet(realPaths:_*)
            .selectExpr(columns:_*)

          //写入表数据
          logDF.write.mode(mode).insertInto(table)
        }
      }
    }catch{
      case ex:Exception => {
        println(s"$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(null != fs){
        fs.close()
      }
    }
  }


  /**
    * 投放任务
    */
  def handleReleaseJob(spark:SparkSession, appName :String, bdp_day:String) :Unit = {
    val begin = System.currentTimeMillis()
    try{
      //缓存级别
      val storageLevel :StorageLevel = ReleaseDataConstant.DEF_STORAGE_LEVEL

      //投放数据处理
      handleReleaseDatas(spark, bdp_day, storageLevel)

    }catch{
      case ex:Exception => {
        println(s"handleReleaseJob occur exception：app=[$appName],date=[${bdp_day}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      println(s"handleReleaseJob End：appName=[${appName}], bdp_day=[${bdp_day}], use=[${System.currentTimeMillis() - begin}]")
    }
  }


  /**
    * 投放任务
    * @param appName
    */
  def handleReleaseJobs(appName :String, bdp_day_begin:String, bdp_day_end:String, range:Int) :Unit = {
    var spark :SparkSession = null
    try{
        //spark配置参数
        val sconf = new SparkConf()
          .set("hive.exec.dynamic.partition","true")
          .set("hive.exec.dynamic.partition.mode","nonstrict")
          .set("spark.sql.shuffle.partitions","10")
          .set("hive.merge.mapfiles","true")
          .set("hive.input.format","org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
          //.set("spark.sql.autoBroadcastJoinThreshold", "50485760")
          .setAppName(appName)
          .setMaster("local[4]")

        //spark上下文会话
        spark = SparkHelper.createSpark(sconf)

        val timeRanges = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)

        for(bdp_day <- timeRanges.reverse){
          val bdp_date = bdp_day.toString

          handleReleaseJob(spark, appName, bdp_date)
        }

    }catch{
      case ex:Exception => {
        println(s"handleReleaseJobs occur exception：app=[$appName],bdp_day=[${bdp_day_begin} - ${bdp_day_end}], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }




  def main(args: Array[String]): Unit = {

    //val Array(appName, bdp_day_begin, bdp_day_end) = args

    val appName: String = "release-2018"
    val bdp_day_begin:String = "2018-07-17"
    val bdp_day_end:String = "2018-07-17"

    //val master: String = "local[4]"
    val begin = System.currentTimeMillis()
    handleReleaseJobs(appName, bdp_day_begin, bdp_day_end, ReleaseDataConstant.RANGE_DAYS)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }

}
