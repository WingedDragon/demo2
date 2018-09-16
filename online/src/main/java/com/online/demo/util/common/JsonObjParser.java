package com.online.demo.util.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;


public class JsonObjParser {
    public static final String dateFormat = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonObjParser.class);
    /**
     * 针对classpath的
     */
    public static final String CLASSPATH_SCHEMA = "classpath:";
    /**
     * 针对文件系统的.
     */
    public static final String FILE_SCHEMA = "file:///";



    public static <T> T parseJson(String confPath, Class<T> confClass) {
        LOGGER.warn("the confPath is :" + confPath + ", the confClass is:" + confClass);
        if (confPath == null) {
            throw new RuntimeException("confPath cannot be null, confClass:" + confClass);
        }
        byte[] configBytes;
        InputStream is = null;
        try {
            if (is == null) {
                is = JsonObjParser.class.getClassLoader().getResourceAsStream(confPath);
            }
            configBytes = ByteStreams.toByteArray(is);
        } catch (IOException e) {
            LOGGER.warn("error happened when parse, the confPath is :" + confPath + ", the confClass is:" + confClass, e);
            throw new RuntimeException(e);
        }
        return JSONObject.parseObject(configBytes, confClass);
    }

    /**
     * 将obj 对象转换成json, 其中对时间格式等进行约束.
     *
     * @param obj
     * @return
     */
    public static String objToJson(Object obj) {
        return JSON.toJSONStringWithDateFormat(obj, dateFormat, SerializerFeature.WriteDateUseDateFormat);
    }


    public static void main(String[] args) throws Exception {

        String confPath = "graph/neo4j-driver-config.json";



    }


}
