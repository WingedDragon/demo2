package com.online.demo.util.es

import java.util

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.collection.mutable

/**
  * Created by finup on 2018/4/2.
  */
object EsStatusMappingUtil {

  val status_mapping_url = "es/status-mapping.json"

  var indexFields: mutable.Map[String, mutable.Map[String,List[String]]] = _

  /**
    * 数据属性配置
    * @return
    */
  def getConfig(): mutable.Map[String, mutable.Map[String,List[String]]] = {
    val configStream = this.getClass.getClassLoader.getResourceAsStream(status_mapping_url)
    if (null == indexFields) {
      indexFields = mutable.Map[String, mutable.Map[String,List[String]]]()

      val mapper = new ObjectMapper()
      val root :JsonNode = mapper.readTree(configStream)
      val elements : util.Iterator[util.Map.Entry[String, JsonNode]] = root.fields

      while(elements.hasNext){
        val indexNode :util.Map.Entry[String, JsonNode] = elements.next()
        var sf : StatusFields = null
        val index :String = indexNode.getKey
        val _fields :JsonNode = indexNode.getValue

        var status :String = null

        val sfFields : util.Iterator[util.Map.Entry[String, JsonNode]] = _fields.fields
        val sfmap : mutable.Map[String,List[String]] = mutable.Map[String,List[String]]()
        while(sfFields.hasNext){
          var fields = List[String]()
          val statusNode :util.Map.Entry[String, JsonNode] = sfFields.next()

          status = statusNode.getKey
          val _stFields :JsonNode = statusNode.getValue
          val stFields = classOf[ArrayNode].cast(_stFields)

          val it = stFields.iterator()
          while(it.hasNext){
            val jn = it.next()
            fields = fields.:+(jn.asText())
          }

          sfmap.+=(status -> fields)
        }

        //indexFields.put(index, sfmap)
        indexFields.+=(index -> sfmap)
      }
    }
    indexFields
  }

  def getFields(index: String): Option[mutable.Map[String,List[String]]] = {
    if(indexFields == null){
      getConfig()
    }
    this.indexFields.get(index)
  }


  class StatusFields(var status: String, var fields: List[String])


  def main(args: Array[String]): Unit = {

    var indexFields: mutable.Map[String, mutable.Map[String,List[String]]] = getConfig()

    println(s"indexFields=$indexFields")

    val index = "release_data"
    val sf : Option[mutable.Map[String,List[String]]] = getFields(index)
    println(s"sf=$sf")

//    sf match{
//      case s:mutable.Map[String,List[String]] => println(s"StatusFields=$s")
//      case _ => println(s"index=$index is not match")
//    }

  }


}
