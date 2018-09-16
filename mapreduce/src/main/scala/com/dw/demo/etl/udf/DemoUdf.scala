package com.dw.demo.etl.udf

import org.apache.commons.lang3.StringUtils
import org.codehaus.jackson.map.ObjectMapper

/**
  * Created by finup on 2018/7/18.
  */
object DemoUdf {

  val objectMapper: ObjectMapper = new ObjectMapper()



  /**
    * 投放请求日志 请求id
    * @param paramJson
    * @return
    */
  def requestSIDUDF(paramJson:String, key:String): String = {
    var fvalue = ""
    if(StringUtils.isNotEmpty(paramJson)){
      val extMap: java.util.Map[String, Object] = objectMapper.readValue(paramJson, classOf[java.util.Map[String, Object]])
      fvalue = extMap.getOrDefault(key,"").toString
    }
    fvalue
  }

}
