package ff.flink.processing.common.es

import java.net.{InetAddress, InetSocketAddress}

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}


/**
  * es配置文件加载工具
  */
object EsConfigUtil {

  var esConfig: EsConfig = null

  def getConfig(configPath: String): EsConfig = {
    val configStream = this.getClass.getClassLoader.getResourceAsStream(configPath)
    if (null == esConfig) {
      val mapper = new ObjectMapper()
      val configJsonObject = mapper.readTree(configStream)

      val configJsonNode = configJsonObject.get("config")

      val config = {
        val configJsonMap = new java.util.HashMap[String, String]
        val it = configJsonNode.fieldNames()
        while (it.hasNext) {
          val key = it.next()
          configJsonMap.put(key, configJsonNode.get(key).asText())
        }
        configJsonMap
      }

      val addressJsonNode = configJsonObject.get("address")
      val addressJsonArray = classOf[ArrayNode].cast(addressJsonNode)
      val transportAddresses = {
        val transportAddresses = new java.util.ArrayList[InetSocketAddress]
        val it = addressJsonArray.iterator()
        while (it.hasNext) {
          val detailJsonNode: JsonNode = it.next()
          val ip = detailJsonNode.get("ip").asText()
          val port = detailJsonNode.get("port").asInt()
          transportAddresses.add(new InetSocketAddress(InetAddress.getByName(ip), port))
        }
        transportAddresses
      }

      esConfig = new EsConfig(config, transportAddresses)
    }
    esConfig
  }

  class EsConfig(var config: java.util.HashMap[String, String], var transportAddresses: java.util.ArrayList[InetSocketAddress])


}
