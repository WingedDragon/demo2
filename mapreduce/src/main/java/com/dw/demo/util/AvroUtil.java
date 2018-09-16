package com.dw.demo.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang.Validate;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by finup on 2017/11/4.
 */
public class AvroUtil implements Serializable {


        /**
         * schema
         * @param avroPath
         * @return
         * @throws IOException
         */
        public static Schema createSchema(String avroPath) throws IOException {
            Validate.notEmpty(avroPath, "avroPath is not empty");

            List<Schema.Field> fields = new ArrayList<Schema.Field>();
            Schema timeStampField = Schema.create(Schema.Type.LONG);
            fields.add(new Schema.Field("timestamp", timeStampField, null, null));
            Schema resultSchema = Schema.createRecord("MyRecord", null, "org.demo", false, fields);
            System.out.println(resultSchema);

            return new Schema.Parser().parse(AvroUtil.class.getClassLoader().getResourceAsStream(avroPath));
        }


    /**
     * schema
     * @param scheainfo
     * @return
     * @throws IOException
     */
    public static Schema createSchema4String(String scheainfo) throws IOException {
        Validate.notEmpty(scheainfo, "schema is not empty");

        return new Schema.Parser().parse(scheainfo);
    }


        /**
         * 记录序列化
         * @param avroPath
         * @param obj
         * @return
         * @throws Exception
         */
        public static byte[] serializeble(String avroPath, Object obj) throws Exception {
            Validate.notEmpty(avroPath, "avroPath is empty");
            Validate.notNull(obj, "obj is Null");

            ByteArrayOutputStream bos = null;
            byte[] serializedValue = null;
            try {
                Schema schema = createSchema(avroPath);

                GenericRecord record = createGenericRecord(schema, obj);

                bos = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
                GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
                writer.write(record, encoder);
                encoder.flush();
                serializedValue = bos.toByteArray();
            } catch (Exception ex) {
                throw ex;
            } finally {
                if (bos != null) {
                    try {
                        bos.close();
                    } catch (Exception e) {
                        bos = null;
                    }
                }
            }

            return serializedValue;
        }


        public static Map<String,Object> deserializeble(String avroPath, byte[] datas) throws Exception {
            Map<String,Object> result = new HashMap<String,Object>();
            InputStream is = null;
            try {
                Schema schema = createSchema(avroPath);

                is = new ByteArrayInputStream(datas);
                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(is, null);

                GenericRecord record = reader.read(null,decoder);

                List<Schema.Field> fields = schema.getFields();
                for(Schema.Field field : fields){
                    String fieldName = field.name();
                    Object filedValue = record.get(fieldName);

                    result.put(fieldName, filedValue);
                }

            } catch (Exception ex) {
                throw ex;
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch (Exception e) {
                        is = null;
                    }
                }
            }

            return result;
        }

        public static GenericRecord deserializeble4Record(String avroPath, byte[] datas) throws Exception {
            GenericRecord result = null;
            InputStream is = null;
            try {
                Schema schema = createSchema(avroPath);

                is = new ByteArrayInputStream(datas);
                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(is, null);

                result = reader.read(null,decoder);

            } catch (Exception ex) {
                throw ex;
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch (Exception e) {
                        is = null;
                    }
                }
            }

            return result;
        }


        /**
         * 对象转avro数据
         * @param schema
         * @param obj
         * @return
         */
        private static GenericRecord createGenericRecord(Schema schema, Object obj) throws Exception{
            Validate.notNull(schema, "schema is Null");
            Validate.notNull(obj, "obj is Null");

            Map<String,Object> objectDatas =  ReflexUtil.getFildKeyValues(obj);

            GenericRecord record = new GenericData.Record(schema);
            for(Map.Entry<String,Object> entry : objectDatas.entrySet()){
                String fieldName = entry.getKey();
                Object fieldValue = entry.getValue();

                //System.out.println("fieldName===>" + fieldName);
                record.put(fieldName, fieldValue);
            }

            return record;
        }



        public static void main(String[] args){



        }


}
