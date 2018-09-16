package com.online.demo.util.kafka.serializable.kryo;


import com.online.demo.domain.ReleaseBidding;
import com.online.demo.util.constant.ReleaseConstant;
import com.online.demo.util.kryo.KryoUtil;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Administrator on 2017-6-15.
 */
public class DomainDecoding implements org.apache.kafka.common.serialization.Deserializer<Serializable> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Serializable deserialize(String topic, byte[] data) {
        Serializable obj = null;
        try{
            if(null != data){
                if(ReleaseConstant.TOPIC_TEST.equalsIgnoreCase(topic)){
                    obj = KryoUtil.deserializationObject(data, ReleaseBidding.class);
                }

            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return obj;
    }

    @Override
    public void close() {

    }
}
