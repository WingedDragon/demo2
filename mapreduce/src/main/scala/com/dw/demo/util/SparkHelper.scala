package com.dw.demo.util

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.dw.demo.constant.ReleaseDataConstant
import com.dw.demo.etl.RealeaseJob
import com.dw.demo.etl.udf.DemoUdf
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{Dependency, SparkConf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by finup on 2018/5/14.
  */

case class JDBCCase(url :String, user:String, password:String)

object SparkHelper {

  val logger :Logger = LoggerFactory.getLogger(RealeaseJob.getClass)

  //缓存级别
  var storageLevels :mutable.Map[String,StorageLevel] = mutable.Map[String,StorageLevel]()


  /**
    * 创建sparksession
    * @param sconf
    * @return
    */
  def createSpark(sconf:SparkConf) :SparkSession = {
    val spark :SparkSession = SparkSession.builder
      .config(sconf)
      .enableHiveSupport()
      .getOrCreate();

    //加载自定义函数
    registerFun(spark)

    spark
  }

  def createSparkNotHive(sconf:SparkConf) :SparkSession = {
    val spark :SparkSession = SparkSession.builder
      .config(sconf)
      .getOrCreate();

    //加载自定义函数
    registerFun(spark)

    spark
  }



  /**
    * udf注册
    * @param spark
    */
  def registerFun(spark: SparkSession):Unit={
    //1 requestUDF
    spark.udf.register("requestUDF", DemoUdf.requestSIDUDF _)
  }


  /**
    * 重分区
    * @param df
    * @return
    */
  def repartitionsDF(df:DataFrame) : DataFrame = {
    val partitions = df.rdd.partitions.length
    val childPartitions :Int = partitions./(ReleaseDataConstant.DEF_PARTITIONS_FACTOR)
    //println(s"parent.partitions=${partitions},childPartitions=${childPartitions}")
    df.repartition(childPartitions)
  }


  /**
    * 依赖关系
    * @param df
    * @return
    */
  def dependenciesRelation(df:DataFrame, dfName:String) : Unit = {
    val dependencies:Seq[Dependency[_]] = df.rdd.dependencies
    println(s"df[${dfName}]=${dependencies}")
  }

  /**
    * 数据转换
    */
  def convertData(values :mutable.Map[String,Object], statusFields: Seq[String]): mutable.Map[String, Object] ={
    values.filter(schemaField => {
      val key = schemaField._1
      statusFields.contains(key)
    })
  }



  /**
    * 时间格式化
    * @param timestamp
    * @param formatter
    * @return
    */
  def formatDate(timestamp:Long, formatter:String="yyyyMMdd"):String={
    var formatDate :Date = new Date(timestamp)
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat(formatter)
    var hehe = dateFormat.format(formatDate)
    hehe
  }






  /**
    * 参数校验
    * @return
    */
  def rangeDate(day:String, range:Int):Seq[String] = {
    val bdp_days =  new ArrayBuffer[String]()
    try{
      val bdp_date_begin = day.toInt
      var cday = day
      var loop = 0

      bdp_days.+=(day)
      while(range > loop){
        val pday = DateUtil.dateFormat4StringDiff(cday, -1)
        bdp_days.+=(pday)
        cday = pday
        loop += 1
      }

    }catch{
      case ex:Exception => {
        println(s"$ex")
        logger.error(ex.getMessage, ex)
      }
    }

    bdp_days
  }


  /**
    * 参数校验
    * @return
    */
  def rangeDates(begin:String, end:String):Seq[String] = {
    val bdp_days =  new ArrayBuffer[String]()
    try{
      val bdp_date_begin = DateUtil.dateFormat4String(begin,"yyyy-MM-dd")
      val bdp_date_end = DateUtil.dateFormat4String(end,"yyyy-MM-dd")

      if(begin.equals(end)){
        bdp_days.+=(bdp_date_begin)
      }else{
        var cday = bdp_date_begin
        while(cday != bdp_date_end){
          bdp_days.+=(cday)
          val pday = DateUtil.dateFormat4StringDiff(cday, 1)
          cday = pday
        }
      }

    }catch{
      case ex:Exception => {
        println(s"$ex")
        logger.error(ex.getMessage, ex)
      }
    }

    bdp_days
  }




  /**
    * 创建schema
    * @param schema
    * @param delFields
    * @param addFields
    * @return
    */
  def createSchema(schema: StructType, delFields:Array[String], addFields: Seq[StructField]): StructType = {
    val fields = schema.fields
    val newFields = createFields(fields, delFields, addFields)
    new StructType(newFields)
  }


  /**
    * 构造新schema
    * @param schema
    * @return
    */
  def createSchema(schema: StructType, useFields:Seq[String]): StructType = {
    val fields = schema.fields
    val newFieldList :mutable.Buffer[StructField] = mutable.Buffer[StructField]()
    val filterFields = fields.filter((field:StructField) => {
      useFields.contains(field.name)
    })

    newFieldList.++=(filterFields)
    new StructType(newFieldList.toArray)
  }

  /**
    * 创建新字段
    * @param fields
    * @param delFields
    * @param addFields
    * @return
    */
  def createFields(fields:Array[StructField], delFields:Array[String], addFields: Seq[StructField]): Array[StructField] ={
    val newFieldList :mutable.Buffer[StructField] = mutable.Buffer[StructField]()
    val filterFields = fields.filter((field:StructField) => {
      !delFields.contains(field.name)
    })
    newFieldList.++=(filterFields)
    for(addField <- addFields){
      newFieldList.+=(addField)
    }
    newFieldList.toArray
  }

  def parentPath(path :String) :String = {
    var parentPath = path
    if(StringUtils.isNotBlank(path)){
      parentPath = path.substring(0, path.lastIndexOf('/')+1)
    }
    parentPath
  }



  def readJDBC(path:String): JDBCCase ={
    var jdbc :JDBCCase= null
    if(StringUtils.isNotBlank(path)) {
      val pro:Properties = PropertyUtil.readProperties(path)
      val user = pro.getProperty("user")
      val password = pro.getProperty("password")
      val url = pro.getProperty("url")

      jdbc = JDBCCase(url, user, password)
    }
    jdbc
  }


  /**
    * mysql存储聚合数据
    * @param spark
    * @param df
    */
  def hive2mysql(spark:SparkSession, df:DataFrame, tableName:String, columns:Seq[String]): Unit ={

    //写入mysql
    val mysqlDF = df.selectExpr(columns:_*)

    //mysql
    val jdbcConf : JDBCCase = readJDBC("jdbc.properties")
    if(null != jdbcConf){
      val url = jdbcConf.url
      val user = jdbcConf.user
      val password = jdbcConf.password

      val prop = new Properties()
      prop.put("user", user)
      prop.put("password", password)

      mysqlDF.write.mode(SaveMode.Append).jdbc(url,tableName,prop)
    }
  }




  def main(args: Array[String]): Unit = {



    //val Array(appName, hdfsPath) = args
    val hdfsPath = "/data/ff_platform/interface_logs/get_ad_detail_for*"

    //val parent = parentPath(hdfsPath)

    //println(s"parent=$parent, hdfsPath=${hdfsPath}")


//    val begin = "20180428"
//    val end = "20180501"
//    val rangs = rangeDates(begin:String, end:String)
//    println(s"rangs=$rangs")
//
//
//    val rs = rangeDate(begin, 2)
//    println(s"rangs=$rs")

  }


}
