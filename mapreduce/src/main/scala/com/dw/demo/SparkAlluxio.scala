package com.dw.demo

import com.dw.demo.constant.ReleaseDataConstant
import com.dw.demo.etl.RealeaseJob
import com.dw.demo.etl.RealeaseJob.{handleReleaseJob, handleReleaseJobs, logger}
import com.dw.demo.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by finup on 2018/8/2.
  */

class SparkAlluxio{

}

object SparkAlluxio {

  val logger :Logger = LoggerFactory.getLogger(SparkAlluxio.getClass)


  /**
    * alluxio 数据存储
    * @param appName
    */
  def doneAlluxioDatas(appName :String) :Unit = {
    var spark :SparkSession = null
    try{
      //spark配置参数
      val sconf = new SparkConf()
        .set("hive.exec.dynamic.partition","true")
        .set("hive.exec.dynamic.partition.mode","nonstrict")
        .set("spark.sql.shuffle.partitions","10")
        .set("hive.merge.mapfiles","true")
        //.set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .setAppName(appName)
        .setMaster("local[4]")

      //spark上下文会话
      spark = SparkHelper.createSparkNotHive(sconf)

      //alluxio文件
      val df = spark.read.text("alluxio://localhost:19998/test/demo.txt")
      df.show(100, false)

      //parquet文件
      val df2 = spark.read.parquet("alluxio://localhost:19998/hdfs2/20180715.parquet")
      df2.show(100, false)



    }catch{
      case ex:Exception => {
        println(s"handleReleaseJobs occur exception：app=[$appName]], msg=$ex")
        logger.error(ex.getMessage, ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }


  def main(args: Array[String]): Unit = {


    val appName: String = "spark-alluxio"
    val begin = System.currentTimeMillis()
    doneAlluxioDatas(appName)
    val end = System.currentTimeMillis()

    println(s"appName=[${appName}], begin=$begin, use=${end-begin}")
  }


}
