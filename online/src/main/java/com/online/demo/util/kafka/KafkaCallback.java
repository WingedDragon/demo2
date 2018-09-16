package com.online.demo.util.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by hp on 2017/3/28.
 */
public class KafkaCallback implements Callback {



    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        int partition = metadata.partition();
        String topic = metadata.topic();
        long offset = metadata.offset();

        System.out.println("topic=" + topic + ",partition=" + partition + ",offset=" + offset);
    }

}
