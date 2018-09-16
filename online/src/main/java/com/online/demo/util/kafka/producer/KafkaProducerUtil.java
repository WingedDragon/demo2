package com.online.demo.util.kafka.producer;


import com.online.demo.domain.ReleaseBidding;
import com.online.demo.util.common.CommonUtil;
import com.online.demo.util.constant.ReleaseConstant;
import com.online.demo.util.kafka.KafkaUtil;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.util.*;

/**
 * Created by hp on 2017/5/19.
 */
public class KafkaProducerUtil {

    /**
     * json数据
     * @param path
     * @param topic
     * @param datas
     */
    public static void sendMsg(String path, String topic, Map<String,String> datas){
        Validate.notEmpty(path, "kafka config path is not empty");
        Validate.notEmpty(topic, "topic is not empty");
        Validate.notNull(datas, "datas is not empty");

        try{
            KafkaProducer producer = KafkaUtil.createProducer(path);
            if(null != producer){
                List<String> lines = new ArrayList<String>();
                for(Map.Entry<String, String> entry : datas.entrySet()){
                    String key = entry.getKey();
                    String value = entry.getValue();

                    System.out.println(key);

                    //System.out.println("Producer.key=" + key + ",value=" + value);
                    String line  = "time="+ CommonUtil.formatDate4Def(new Date())+",key="+key+"],value="+value;
                    lines.add(line);

                    producer.send(new ProducerRecord<String, String>(topic, key, value));
                    producer.flush();
                }

                producer.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }




    /**
     * kryo序列化
     * @param path
     * @param topic
     * @param datas
     */
    public static void sendMsg4Kryo(String path, String topic, Map<String,Serializable> datas){
        Validate.notEmpty(path, "kafka config path is not empty");
        Validate.notEmpty(topic, "topic is not empty");
        Validate.notNull(datas, "datas is not empty");

        try{
            Properties props = KafkaUtil.readKafkaProps(path);
            KafkaProducer<String, Serializable>  producer = new KafkaProducer<String, Serializable>(props);

            if(null != producer){

                for(Map.Entry<String, Serializable> entry : datas.entrySet()){
                    String key = entry.getKey();
                    Serializable value = entry.getValue();
                    System.out.println("Producer["+topic+"].key=" + key + ",value=" + value);

                    producer.send(new ProducerRecord<String, Serializable>(topic, key, value));
                    producer.flush();
                }

                producer.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }



    //==========================================================================

    public static void testJson() throws Exception{
        String path = "kafka/json/kafka-producer.properties";

        //发送序列化对象
        int idx = 1;
        while (idx <= 100) {
            String topic4Test = ReleaseConstant.TOPIC_TEST;

            Map<String, String> datas4Bidding = ReleaseBidding.createReleaseBidding4Kafka();
            sendMsg(path, topic4Test, datas4Bidding);

            System.out.println("kafka producer send =" + CommonUtil.formatDate4Def(new Date()));

            Thread.sleep(5000);
            idx++;
        }
    }




    public static void main(String[] args) throws  Exception {


        //testSQL();

        //testEmployee();

        //testPerson();

        //testJson();

        testJson();


        //testKryo();

    }
}
