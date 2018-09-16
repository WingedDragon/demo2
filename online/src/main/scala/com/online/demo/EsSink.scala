package com.online.demo

import java.net.InetSocketAddress
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.online.demo.util.constant.ReleaseConstant
import com.online.demo.util.es.{EsConstant, EsStatusMappingUtil}
import ff.flink.processing.common.es.{Es6ClientUtil, EsIndexConfigUtil}
import ff.flink.processing.common.es.EsIndexConfigUtil.DetailIndex
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.ActionFuture
import org.elasticsearch.action.bulk.{BulkRequestBuilder, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Response
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * 自定义 es sink 处理sessionRecord宽表
  * Created by dl on 2018/3/31.
  */
class EsSink() extends RichSinkFunction[mutable.Map[String, Object]] {

  val logger :Logger = LoggerFactory.getLogger(this.getClass)

  var transportClient: PreBuiltTransportClient = _


  val es_config_url = "es/es-config.json"
  val status_mapping_url = "es/status-mapping.json"

  //配置key
  val indexConfigName = "es_release"
  var detailIndex: DetailIndex = _

  //索引 类型
  val index_summary = "summary"
  val index_detail = "detail"

  //状态
  val onlineStatus = getOnlineStatus()
  val offlineStatus = getOfflineStatus()

  //struct 名称
  val allStatusKey = getAllStatusKey()

  //不覆盖key
  val mutableKeys = getMutableKey()


  //status_mapping
  val status_mapping :mutable.Map[String, mutable.Map[String,List[String]]] = EsStatusMappingUtil.getConfig()



  override def open(parameters: Configuration): Unit = {
    //所用es index及type信息
    detailIndex = EsIndexConfigUtil.getIndex(indexConfigName)

    //不同status对应field信息
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    transportClient = Es6ClientUtil.buildTransportClient(es_config_url)
    super.open(parameters)
  }

  override def invoke(value: mutable.Map[String, Object], context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val checkResult: String = checkData(value)
      if (StringUtils.isNotBlank(checkResult)) {
        //日志记录
        logger.error("EsRecord.sink.checkData.err{}", checkResult)
        return
      }

      //es时间
      val currentTime = System.currentTimeMillis().asInstanceOf[Number]

      //sessionid
      val sessionId = value.getOrElse(ReleaseConstant.SESSION_ID,"").toString

      //target
      var target = ""
      val targetObject = value.getOrElse(ReleaseConstant.TARGET,"").toString
      if (StringUtils.isNotBlank(targetObject)) {
        target = targetObject.toString
      }

      //status
      val status = value.getOrElse(ReleaseConstant.STATUS,"").toString

      //dbTable
      val dbTable = value.getOrElse(ReleaseConstant.DB_TABLE,"").toString

      //es index id
      val id: String = createESID(sessionId, target, status, dbTable)

      //索引名称、类型名称
      val idxName = detailIndex.index
      val idxTypeName = detailIndex.indexType

      val release_status = ReleaseConstant.RELEASE_BIDDING

      dealDBTable(id, dbTable, release_status, idxName , idxTypeName, sessionId ,
        value, currentTime)



    }catch{
      case ex: Exception => logger.error(ex.getMessage)
    }
  }

  /**
    * es id
    * @param sessionId
    * @param target
    * @return
    */
  def createESID(sessionId:String, target:String, status:String, dbTable :String): String ={
    var id = ""
    if(ReleaseConstant.DB_TABLE.equals(dbTable)){

      if(onlineStatus.contains(status)){
        id = sessionId
      }else {
        if(StringUtils.isNotBlank(target)){
          id = sessionId + "_" + target
        }else{
          id = sessionId + "_unknown"
        }
      }

    }else {

      id = sessionId

    }

    id
  }

  /**
    * 消息处理
    */
  def dealDBTable(id: String, dbTable:String, status:String, idxName :String, idxTypeName :String, sessionId :String,
                  value: mutable.Map[String, Object], currentTime:Number): Unit ={

    //多索引
    val idxName1 = idxName + "_" + index_summary
    val idxName2 = idxName + "_" + index_detail

    /**
      * 根据业务选择处理
      */
    if(StringUtils.isNotBlank(dbTable) && StringUtils.isNotBlank(status)) {
      val statusDatas: mutable.Map[String, Object] = chooseData4Status(idxName, status, value)
      if (statusDatas.isEmpty) {
        logger.error("EsRecord.essink.statusData.err index=" + idxName1 + ",status=" + status)
      }

      val mutableKeys:List[String] =  List[String]("ut","bidding_count")
      dealStatus(idxName1, idxTypeName, id, statusDatas, currentTime, mutableKeys)
    }
  }


  /**
    * es列赋值
    * @param idxName
    * @param idxTypeName
    * @param sessionId
    * @param value
    * @param currentTime
    * @return
    */
  def dealStatus(idxName :String, idxTypeName :String, sessionId :String,
                 value: mutable.Map[String, Object], currentTime:Number): Unit ={
    val params = new java.util.HashMap[String, Object]

    val scriptSb: StringBuilder = new StringBuilder
    for ((k, v) <- value if(null != k); if(null != v)) {
      params.put(k, v)

      var s = ""
      if(ReleaseConstant.UT.equals(k)) {
        s = "if(ctx._source."+k+" == null){ctx._source."+k+" = params."+k+"} else { if(ctx._source."+k+" < params."+k+" ){ctx._source."+k+" = params."+k+"}}"
      }else if(allStatusKey.contains(k)){
        //struct 次数累计 时间更新
        var s = ""
        k match {
          case ReleaseConstant.COUNT => s = "if(ctx._source."+k+" != null) {ctx._source."+k +"+= params." + k + "} else { ctx._source."+k+" = params."+k+"} "
          case _ => println(k)
        }

        scriptSb.append(s)
      }else {
        s = "if(ctx._source."+k+" == null){ctx._source."+k+" = params."+k+"}"
      }
      scriptSb.append(s)
    }

    val scripts = scriptSb.toString()
    val script = new Script(ScriptType.INLINE, "painless", scripts, params)
    println(s"script=$script")

    val indexRequest = new IndexRequest(idxName, idxTypeName, sessionId).source(params)
    val response = transportClient.prepareUpdate(idxName, idxTypeName, sessionId)
      .setScript(script)
      .setRetryOnConflict(EsConstant.retryNumber)
      .setUpsert(indexRequest)
      .get()
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      System.err.println("calculate es session record error!map:" + new ObjectMapper().writeValueAsString(value))
      throw new Exception("run script exception:status:" + response.status().name())
    }
  }


  /**
    * 可变列
    * @param idxName
    * @param idxTypeName
    * @param sessionId
    * @param value
    * @param currentTime
    * @param mutableKeys
    */
  def dealStatus(idxName :String, idxTypeName :String, sessionId :String,
                 value: mutable.Map[String, Object], currentTime:Number, mutableKeys:List[String]): Unit ={
    val params = new java.util.HashMap[String, Object]

    val scriptSb: StringBuilder = new StringBuilder
    for ((key, value) <- value if(null != key); if(null != value)) {
      params.put(key, value)

      var s = ""
      if(mutableKeys.contains(key)) {
        if(key.equals(ReleaseConstant.UT)){
          s = "if(ctx._source."+key+" == null){ctx._source."+key+" = params."+key+"} else { if(ctx._source."+key+" < params."+key+" ){ctx._source."+key+" = params."+key+"}}"
        }else{
          s = "if(ctx._source."+key+" == null){ctx._source."+key+" = params."+key+"} else { ctx._source."+key+" += params."+key+"}}"
        }

      }else {
        s = "if(ctx._source."+key+" == null){ctx._source."+key+" = params."+key+"}"
      }
      scriptSb.append(s)
    }

    val scripts = scriptSb.toString()
    println(scripts)
    val script = new Script(ScriptType.INLINE, "painless", scripts, params)
    //println(s"script=$script")

    val indexRequest = new IndexRequest(idxName, idxTypeName, sessionId).source(params)
    val response = transportClient.prepareUpdate(idxName, idxTypeName, sessionId)
      .setScript(script)
      .setRetryOnConflict(EsConstant.retryNumber)
      .setUpsert(indexRequest)
      .get()
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      System.err.println("calculate es session record error!map:" + new ObjectMapper().writeValueAsString(value))
      throw new Exception("run script exception:status:" + response.status().name())
    }
  }



  override def close() = {
    if (this.transportClient != null) {
      this.transportClient.close()
    }
  }


  /**
    * 参数校验
    * @param value
    * @return
    */
  def checkData(value: mutable.Map[String, Object]): String ={
    var msg = ""
    if(null == value){
      msg = "kafka.value is empty"
    }

    //sessionId
    val sessionIdNode = value.get(ReleaseConstant.SESSION_ID)
    if(null == sessionIdNode){
      msg = "EsRecordCalculate.topic.sessionId is null"
    }

    //状态
    val statusNode = value.get(ReleaseConstant.STATUS)
    if(null == statusNode){
      msg = "EsRecordCalculate.topic.status is null"
    }

    //时间
    val ctNode = value.get(ReleaseConstant.CT)
    if(null == ctNode){
      msg = "EsRecordCalculate.topic.ct is null"
    }

    //dbTable
    val dbTableNode = value.get(ReleaseConstant.DB_TABLE)
    if(null == dbTableNode){
      msg = "EsSessionRecordCalculate.topic.dbtable is null"
    }

    msg
  }

  /**
    * 线上状态
    * @return
    */
  def getOnlineStatus() : List[String] = {
    var onlineStatus :List[String] = List[String]()

    onlineStatus = onlineStatus.:+(ReleaseConstant.RELEASE_CUSTOMER)
    onlineStatus = onlineStatus.:+(ReleaseConstant.RELEASE_BIDDING)
    onlineStatus = onlineStatus.:+(ReleaseConstant.RELEASE_SHOW)
    onlineStatus = onlineStatus.:+(ReleaseConstant.RELEASE_CLICK)
    onlineStatus = onlineStatus.:+(ReleaseConstant.RELEASE_ARRIVE)

    onlineStatus
  }

  /**
    * 线上状态
    * @return
    */
  def getOfflineStatus() : List[String] = {
    var offlineStatus :List[String] = List[String]()
    offlineStatus
  }

  /**
    * 全流程列表
    * @return
    */
  def getAllStatusKey() : List[String] = {
    var allStatusKey :List[String] = List[String]()

    allStatusKey = allStatusKey.:+(ReleaseConstant.RELEASE_CUSTOMER)
    allStatusKey = allStatusKey.:+(ReleaseConstant.RELEASE_BIDDING)
    allStatusKey = allStatusKey.:+(ReleaseConstant.RELEASE_SHOW)
    allStatusKey = allStatusKey.:+(ReleaseConstant.RELEASE_CLICK)
    allStatusKey = allStatusKey.:+(ReleaseConstant.RELEASE_ARRIVE)

    allStatusKey
  }

  /**
    * 可变属性
    * @return
    */
  def getMutableKey() : List[String] = {
    var notMutableKeys :List[String] = List[String]()
    notMutableKeys = notMutableKeys.:+(ReleaseConstant.UT)
    notMutableKeys
  }



  /**
    * 选择status对应的schema数据
    * @param index
    * @param status
    * @param value
    * @return
    */
  def chooseData4Status(index:String, status :String,value: mutable.Map[String, Object]) : mutable.Map[String, Object] = {

    var filterDatas : mutable.Map[String, Object] = mutable.Map[String, Object]()
    val indexFields : Option[mutable.Map[String,List[String]]] = status_mapping.get(index)

    indexFields match {
      case idxFields:Some[mutable.Map[String,List[String]]] => {
        val statusFields : List[String] = idxFields.get(status)
        filterDatas = value.filter(entry => {
          val field = entry._1
          statusFields.contains(field)
        })
      }
      case _ => logger.error("EsRecord.essink.chooseData4Status.err status={}", status)
    }

    filterDatas

  }



}
