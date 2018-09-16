package ff.flink.processing.common.es

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

/**
  * es 索引配置
  */
object EsIndexConfigUtil {
  val configPath: String = "es/es-index-config.json"

  var indexes: java.util.HashMap[String, DetailIndex] = _

  def getConfig(): java.util.HashMap[String, DetailIndex] = {
    val configStream = this.getClass.getClassLoader.getResourceAsStream(configPath)
    if (null == indexes) {
      val mapper = new ObjectMapper()
      val configJsonObject = mapper.readTree(configStream)

      val configJsonNode = configJsonObject.get("config")
      val configJsonArray = classOf[ArrayNode].cast(configJsonNode)

      indexes = {
        val indexes: java.util.HashMap[String, DetailIndex] = new java.util.HashMap[String, DetailIndex]
        val it = configJsonArray.iterator()
        while (it.hasNext) {
          val detailJsonNode: JsonNode = it.next()
          val detailIndex = new DetailIndex(detailJsonNode.get("index").asText(), detailJsonNode.get("type").asText())
          indexes.put(detailJsonNode.get("name").asText(), detailIndex)
        }
        indexes
      }
    }
    indexes
  }

  def getIndex(name: String): DetailIndex = {
    if(indexes == null){
      getConfig()
    }
    this.indexes.get(name)
  }

  class DetailIndex(val inIndex: String, val inIndexType: String) extends Serializable{
    var index = inIndex
    var indexType = inIndexType
  }

}
