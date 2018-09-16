package com.dw.demo.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.*;

/**
 * Created by finup on 2017/11/4.
 */
public class ParquetUtil<T extends IndexedRecord> {


    private Schema schema;

    public ParquetUtil(Schema schema) {
        this.schema = schema;
    }

    ParquetFileWriter.Mode WRITE = ParquetFileWriter.Mode.OVERWRITE;


    CompressionCodecName codec = ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME;  //CompressionCodecName.GZIP;
    boolean enableDictionary = false;
    boolean validating = false;
    int blockSize = ParquetWriter.DEFAULT_BLOCK_SIZE / 1024;


    /**
     * 读取parquet文件
     * @param filePath
     * @throws Exception
     */
    public static List<String> readParquetFile2Json(String filePath) throws Exception {
        AvroParquetReader<GenericRecord> reader = null;
        List<String> parquets = Lists.newArrayList();
        try{
            if(!StringUtils.isEmpty(filePath)){
                Path path = new Path(filePath);
                reader= new AvroParquetReader<GenericRecord>(path);
                GenericRecord nextRecord = null;
                while ((nextRecord = reader.read()) != null) {
                    Schema schema = nextRecord.getSchema();
                    System.out.println(schema);

                    String recordJSON = JSONObject.toJSONString(nextRecord);
                    parquets.add(recordJSON);
                    System.out.println("recordJSON=" + recordJSON);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(null != reader){
                reader.close();
            }
        }
        return parquets;
    }


    /**
     * 读取parquet文件
     * @param filePath
     * @throws Exception
     */
    public static List<String> readParquetFile(String filePath) throws Exception {
        AvroParquetReader<GenericRecord> reader = null;
        List<String> parquets = Lists.newArrayList();
        try{
            if(!StringUtils.isEmpty(filePath)){
                Path path = new Path(filePath);
                reader= new AvroParquetReader<GenericRecord>(path);
                GenericRecord nextRecord = null;
                while ((nextRecord = reader.read()) != null) {
                    Schema schema = nextRecord.getSchema();
                    //System.out.println(schema);

                    List<Schema.Field> fields = schema.getFields();
                    Map<String,String> ffs = Maps.newHashMap();
                    for(Schema.Field field : fields){
                        String fieldName = field.name();
                        Object fieldValue = nextRecord.get(fieldName);
                        String fieldValueStr = null == fieldValue ? "":JSONObject.toJSONString(fieldValue);
                        ffs.put(fieldName, fieldValueStr);
                        //System.out.println("fieldName=" + fieldName + ",fieldValue=" + fieldValue);
                    }

                    System.out.println("json=" + JSONObject.toJSONString(ffs));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(null != reader){
                reader.close();
            }
        }
        return parquets;
    }

    /**
     * 写出parquet文件
     * @param output
     * @param schema
     * @param records
     * @throws Exception
     */
    public static void writeParquetFile(String output, Schema schema, List<GenericRecord> records) throws Exception {
        //输出路径
        Path outputPath = new Path(output);
        AvroParquetWriter<GenericRecord> writer = null;
        try {
            writer = new AvroParquetWriter<GenericRecord>(outputPath,schema);
            for(GenericRecord record : records){
                writer.write(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(null != writer){
                writer.close();
            }
        }
    }


    private static Object convert(String type, Object value) throws Exception{
        Object newValue = new Object();
        if(!StringUtils.isEmpty(type) && null != value){
            if(Schema.Type.STRING.getName().equals(type)){
                newValue = value.toString();
            }else if(Schema.Type.INT.getName().equals(type)){
                newValue = Integer.valueOf(value.toString());
            }else if(Schema.Type.LONG.getName().equals(type)){
                newValue = Integer.valueOf(value.toString());
            }else if(Schema.Type.DOUBLE.getName().equals(type)){
                newValue = Integer.valueOf(value.toString());
            }else if(Schema.Type.FLOAT.getName().equals(type)){
                newValue = Integer.valueOf(value.toString());
            }else if(Schema.Type.ARRAY.getName().equals(type)){
                newValue = Lists.newArrayList(value);
            }
        }
        return newValue;
    }


    public static GenericRecord createGenericRecord(String output, String json) throws Exception {

        GenericRecord record = null;
        if(!StringUtils.isEmpty(output) && !StringUtils.isEmpty(json)){
            //输出路径
            Path outputPath = new Path(output);

            Map values = JSONObject.parseObject(json, HashMap.class);

            Schema schema = new Schema.Parser().parse(ParquetUtil.class.getClassLoader().getResourceAsStream("avro/session_record.avsc"));
            AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(outputPath,schema);
            List<Schema.Field> fields = schema.getFields();
            record = new GenericData.Record(schema);

            Object newObj = null;
            for(Schema.Field field : fields){
                String fieldName = field.name();
                Schema fieldSchema = field.schema();
                String type = schema.getType().getName();

                //转化
                Object value = values.get(fieldName);
                if(null != value){
                    newObj = convert(type, values.get(fieldName));
                }
                record.put(fieldName, newObj);

            }
        }
        return record;
    }

    //===========================================================






    /**
     * 写出parquet文件
     * @param output
     * @param rows
     * @throws Exception
     */
    public static void writeTest4Employee(String output, int rows) throws Exception {
        //输出路径
        Path outputPath = new Path(output);

        Schema schema = new Schema.Parser().parse(ParquetUtil.class.getClassLoader().getResourceAsStream("avro/employee.avsc"));
        AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(outputPath,schema);
        for (int i=0; i < rows; i++) {
            GenericRecord record = new GenericData.Record(schema);

            String name = CommonUtil.getRandomChar(3);
            record.put("id", String.valueOf(CommonUtil.getRandomNum(4)));
            record.put("name", name);
            record.put("age", CommonUtil.getRandomNum(2));
            record.put("birthday", CommonUtil.getRandomDate(Calendar.DAY_OF_MONTH, 100));
            record.put("nation", CommonUtil.getRandomNation());
            record.put("salary", CommonUtil.getRandomDouble(2,1));
            record.put("loves", CommonUtil.getRandomLikes2(3));
            record.put("info", CommonUtil.getRandomInfo(name));
            record.put("family", CommonUtil.getRandomFamily(name));
            record.put("num", CommonUtil.getRandomNum(2));
            try {
                writer.write(record);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        writer.close();
    }


    /**
     * 写出parquet文件
     * @param output
     * @param rows
     * @throws Exception
     */
    public static void writeTest4Release(String output, int rows) throws Exception {
        //输出路径
        Path outputPath = new Path(output);

        Schema schema = new Schema.Parser().parse(ParquetUtil.class.getClassLoader().getResourceAsStream("avro/release.avsc"));
        AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(outputPath,schema);
        for (int i=0; i < rows; i++) {
            GenericRecord record = new GenericData.Record(schema);

            //时间参数、时间范围、时间格式化
            int dateType = Calendar.DAY_OF_MONTH;
            int dateRange = 10;
            String dateFormatter = "yyyyMMddHHmmss";
            Date ctDate = ReleaseHelper.getRandomDate(dateType, dateRange);
            long ctTime = ctDate.getTime();
            String date = ReleaseHelper.formatDate4Def(ctDate, dateFormatter);

            String reqRandoms = ReleaseHelper.getRandomChar(6);
            String sessionRandoms = ReleaseHelper.getRandomChar(6);

            //请求id
            String reqId = date + reqRandoms;
            record.put("reqid", reqId);

            //会话id
            String sessionId = date + sessionRandoms;
            record.put("sessionid", sessionId);

            //设备号
            String device = ReleaseHelper.getRandomChar(3);
            record.put("device", device);

            //渠道
            String source = ReleaseHelper.getSource();
            record.put("source", source);

            //渠道
            String status = ReleaseHelper.getStatus();
            record.put("status", status);

            //时间
            record.put("ct", ctTime);

            //其他参数
            Map exts = ReleaseHelper.getExts(status);
            record.put("exts", exts);

            //其他环节list
            List<GenericRecord> others = Lists.newArrayList();
            if(ReleaseHelper.RELEASE_CUSTOMER.equalsIgnoreCase(status)){
                others = getOtherRecord4Release(record, schema);
            }

            List<GenericRecord> records = Lists.newArrayList();
            records.add(record);
            records.addAll(others);
            try {
                for(GenericRecord rec : records){
                    writer.write(rec);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        writer.close();
    }

    /*
       竞价记录
     */
    public static List<GenericRecord> getOtherRecord4Release(GenericRecord record, Schema schema) throws Exception {
        List<GenericRecord> records = Lists.newArrayList();

        if(null != record){

            int random =  new Random().nextInt(2)%2;
            int random2 =  new Random().nextInt(2)%2;

            //竞价
            String biddingStatus = ReleaseHelper.RELEASE_BIDDING;
            GenericRecord biddingRecord = new GenericData.Record(schema);
            biddingRecord.put("reqid", record.get("reqid"));
            biddingRecord.put("sessionid", record.get("sessionid"));
            biddingRecord.put("device", record.get("device"));
            biddingRecord.put("source", record.get("source"));
            biddingRecord.put("status", biddingStatus);
            biddingRecord.put("ct", record.get("ct"));

            Map biddingExts = ReleaseHelper.getExts(biddingStatus);
            biddingRecord.put("exts", biddingExts);

            //曝光
            String showStatus = ReleaseHelper.RELEASE_SHOW;
            GenericRecord showRecord = new GenericData.Record(schema);
            showRecord.put("reqid", record.get("reqid"));
            showRecord.put("sessionid", record.get("sessionid"));
            showRecord.put("device", record.get("device"));
            showRecord.put("source", record.get("source"));
            showRecord.put("status", showStatus);
            showRecord.put("ct", record.get("ct"));


            //点击
            String clickStatus = ReleaseHelper.RELEASE_CLICK;
            GenericRecord clickRecord = new GenericData.Record(schema);
            clickRecord.put("reqid", record.get("reqid"));
            clickRecord.put("sessionid", record.get("sessionid"));
            clickRecord.put("device", record.get("device"));
            clickRecord.put("source", record.get("source"));
            clickRecord.put("status", clickStatus);
            clickRecord.put("ct", record.get("ct"));


            if(random == 0){
                records.add(biddingRecord);
                String code = biddingExts.get("code").toString();
                if(ReleaseHelper.BIDDING_SUC.equals(code)){
                    records.add(showRecord);
                    if(random2 == 0){
                        records.add(clickRecord);
                    }
                }
            }

        }
        return records;
    }

    /**
     * 写出parquet文件
     * @param output
     * @param rows
     * @throws Exception
     */
    public static void writeTests(String output, int begin, int end, int rows) throws Exception {
        for(int i=begin;i <= end; i++){
            String subOutput = output+ i+ ".parquet";
            writeTest4Employee(subOutput, rows);
        }
    }





    public static void main(String[] args) throws Exception{

        String json = "";
        String path = "";

        String output = "/demo/datas/20180717.parquet";
        int rows = 300;
        int begin = 1;
        int end = 3;

        writeTest4Release(output, rows);


        String input = "/demo/datas/20180716.parquet";
        //readParquetFile(input);

    }

}
