package ff.flink.processing.common.es

import java.net.InetSocketAddress

import ff.flink.processing.common.es.EsConfigUtil.EsConfig
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Created by finup on 2018/4/9.
  */
object Es6ClientUtil {

  val logger :Logger = LoggerFactory.getLogger(Es6ClientUtil.getClass)


  //es连接
  def buildTransportClient(esConfigPath: String): PreBuiltTransportClient = {
    if (null == esConfigPath) {
      throw new RuntimeException("esConfigPath is null!")
    }
    val esConfig: EsConfig = EsConfigUtil.getConfig(esConfigPath)
    val configs : mutable.Map[String,String] = esConfig.config.asScala
    val transAddrs :mutable.Buffer[InetSocketAddress] = esConfig.transportAddresses.asScala

    //es 参数
    val settings :Settings.Builder = Settings.builder()
    for((key, value) <- esConfig.config.asScala){
      settings.put(key, value)
    }

    val transportClient = new PreBuiltTransportClient(settings.build())
    for(transAddr <- transAddrs){
      val transport : TransportAddress = new TransportAddress(transAddr)
      transportClient.addTransportAddress(transport)
    }


    var info = ""
    if (transportClient.connectedNodes.isEmpty) {
      info = "Elasticsearch client is not connected to any Elasticsearch nodes!"
      logger.error(info)
      throw new RuntimeException(info)
    }else {
      info = "Created Elasticsearch TransportClient with connected nodes {}"+ transportClient.connectedNodes
      logger.info(info)
    }
    transportClient
  }


}


class Es6ClientUtil{

}
