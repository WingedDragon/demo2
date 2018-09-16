package com.dw.demo.graph.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dw.demo.util.JsonObjParser;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.dw.demo.graph.config.NeoServerConfig;
import com.dw.demo.graph.constant.NeoConstant;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.neo4j.driver.v1.*;
import org.slf4j.LoggerFactory;
import scala.Tuple4;
import scala.Tuple4$;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by finup on 2017/11/27.
 */
public class NeoApocService implements AutoCloseable {


    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NeoApocService.class);


    //配置
    private NeoServerConfig neoConfig;

    //连接驱动
    private Driver driver;


    public NeoApocService(String configPath) {
        try {
            neoConfig = JsonObjParser.parseJson(configPath, NeoServerConfig.class);
            if (null != neoConfig) {
                String user = neoConfig.getUser();
                String pass = neoConfig.getPass();
                String url = neoConfig.getUrl();
                int sessions = neoConfig.getSessionSize().intValue();
                long connectTimeout = neoConfig.getConnectTimeout();
                long transRetryTimeout = neoConfig.getTransRetryTimeout();

                Config config = Config.build().withMaxIdleSessions(sessions)
                        .withConnectionTimeout(connectTimeout, TimeUnit.SECONDS)
                        .withMaxTransactionRetryTime(transRetryTimeout, TimeUnit.SECONDS)
                        .toConfig();

                driver = GraphDatabase.driver(url, AuthTokens.basic(user, pass), config);
            }
        } catch (Exception e) {
            logger.error("NeoManager read config err", e);
            throw new RuntimeException(e);
        }
    }


    @Override
    public void close() throws Exception {
        if (null != driver) {
            driver.close();
        }
    }

    public Driver getDriver() {
        return driver;
    }


    /**
     * 增加标签
     * @param cql
     * @param ids
     * @param labels
     */
    public void addLabels(String cql, List<Long> ids, List<String> labels) {
        labels = Arrays.asList("DD", "HH");
        ids = Arrays.asList(100l, 103l, 109l);

        Map<String, Object> params = new HashedMap();
        params.put("labels", labels);
        params.put("ids", ids);

        //按类别匹配?????
        cql = "CALL apoc.create.addLabels({ids}, {labels})";
        try (Session session = driver.session()) {
            try (Transaction tx = session.beginTransaction()) {

                tx.run(cql, Values.value(params));
                tx.success();
            } catch (Exception e) {
                logger.error("createNode run cql err ", cql, e);
                e.printStackTrace();
            }
        } catch(Exception e){
            logger.error("createNode get session err ", e);
            e.printStackTrace();
            }
        }

    /**
     * 创建节点
     * @param labels     Arrays.asList("A","B")
     * @param properties properties.put("name","1");   properties.put("sex","man");
     */
    public Long createNode(List<String> labels, Map<String, Object> properties) {
        Long result = null;

        if(CollectionUtils.isNotEmpty(labels)){
            String cql = "CALL apoc.create.node({labels}, {properties}) yield node return id(node) as nodeid ";

            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {

                    Map<String,Object> parameters = Maps.newHashMap();
                    parameters.put(NeoConstant.Parameter_Labels, labels);
                    parameters.put(NeoConstant.Parameter_Properties, properties);

                    StatementResult strt = tx.run(cql, Values.value(parameters));
                    Record record = strt.single();

                    result = record.get("nodeid").asLong();
                    tx.success();
                } catch (Exception e) {
                    logger.error("createNode run cql err ", cql, e);
                    e.printStackTrace();
                }
            } catch (Exception e) {
                logger.error("createNode get session err ", e);
                e.printStackTrace();
            }
        }
        return result;

    }

    /**
     * 批量创建节点
     * @param labels
     * @param properties
     */
    public List<Long> createNodes(List<String> labels, List<Map<String, Object>> properties) {
        List<Long> ids = Lists.newArrayList();

        if(CollectionUtils.isNotEmpty(labels) && CollectionUtils.isNotEmpty(properties)){

            String cql = "CALL apoc.create.nodes({labels}, {properties}) yield node return id(node) as nodeid";

            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {

                    Map<String, Object> params = new HashedMap();
                    params.put("labels", labels);
                    params.put("properties", properties);

                    StatementResult strt = tx.run(cql, Values.value(params));

                    while(strt.hasNext()){
                        Record record = strt.next();

                        Long id = record.get("nodeid").asLong();
                        ids.add(id);
                    }

                    tx.success();
                } catch (Exception e) {
                    logger.error("createNode run cql err ", cql, e);
                    e.printStackTrace();
                }
            } catch (Exception e) {
                logger.error("createNode get session err ", e);
                e.printStackTrace();
            }
        }

        return ids;
    }



    public Long createNode2(List<String> labels, Map<String, Object> properties) {
        Long result = null;

        if(CollectionUtils.isNotEmpty(labels)){
            String cql = " CALL apoc.create.node({labels}, {properties}) yield node return id(node) as nodeid ";

            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {

                    Map<String,Object> parameters = Maps.newHashMap();
                    parameters.put(NeoConstant.Parameter_Labels, labels);
                    parameters.put(NeoConstant.Parameter_Properties, properties);

                    StatementResult strt = tx.run(cql, Values.value(parameters));
                    Record record = strt.single();

                    result = record.get("nodeid").asLong();
                    tx.success();
                } catch (Exception e) {
                    logger.error("createNode run cql err ", cql, e);
                    e.printStackTrace();
                }
            } catch (Exception e) {
                logger.error("createNode get session err ", e);
                e.printStackTrace();
            }
        }
        return result;

    }



    public Long findNode(String label, Map<String, String> properties) {
        Long result = null;

        if(!StringUtils.isEmpty(label)){

            String cql = "match (n :"+label+" ) with n where return id(n) as nodeid ";
            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {

                    Map<String,Object> parameters = Maps.newHashMap();
                    parameters.put(NeoConstant.Parameter_ID, 4);
                    parameters.put(NeoConstant.Parameter_Key, label);
                    parameters.put(NeoConstant.Parameter_Properties, properties);

                    StatementResult strt = tx.run(cql, Values.value(parameters));
                    Record record = strt.single();

                    result = record.get("nodeid").asLong();
                    tx.success();
                } catch (Exception e) {
                    logger.error("createNode run cql err ", cql, e);
                    e.printStackTrace();
                }
            } catch (Exception e) {
                logger.error("createNode get session err ", e);
                e.printStackTrace();
            }
        }
        return result;

    }




    /**
     * 创建关系
     * @param beginID
     * @param endID
     * @param relType
     * @param relProperties
     */
    public Long addRelationship(Long beginID, Long endID, String relType, Map<String, String> relProperties) {

        Long relID = null;
        if(null != beginID && null != endID && !StringUtils.isEmpty(relType)){
            String cql = " match (begin) WHERE id(begin) = {beginID} " +
                    " match (end) WHERE id(end) = {endID} " +
                    " CALL apoc.create.relationship(begin, {relType}, {relProperties}, end) yield rel " +
                    " return id(rel) as relid ";
            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {

                    Map<String, Object> params = new HashedMap();
                    params.put("beginID", beginID);
                    params.put("endID", endID);
                    params.put("relType", relType);
                    params.put("relProperties", relProperties);

                    StatementResult strt = tx.run(cql, Values.value(params));
                    Record record = strt.single();
                    relID = record.get("relid").asLong();

                    tx.success();
                } catch (Exception e) {
                    logger.error("createNode run cql err ", cql, e);
                    e.printStackTrace();
                }
            } catch(Exception e){
                logger.error("createNode get session err ", e);
                e.printStackTrace();
            }
        }

        return relID;
    }


    /**
     * 创建图结构
     */
    public void createGraph(List datas) {

        if(null != datas){
            Map<String, Object> params = Maps.newHashMap();
            params.put("batch", datas);

            String cql = "UNWIND {batch} as row \n" +
                    "merge (b {name:row.beginNode}) \n" +
                    "merge (e {name:row.endNode}) \n" +
                    "merge (b)-[r:Unkonw1]-(e) set r=row.relproperty";

            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {
                    StatementResult strt = tx.run(cql,Values.value(params));

//                    Record record = strt.single();
//                    Map resutMap = record.asMap();
//                    System.out.println(resutMap);

                    tx.success();
                } catch (Exception e) {
                    logger.error("createGraph run cql err ", cql, e);
                    e.printStackTrace();
                }
            } catch (Exception e) {
                logger.error("createGraph get session err ", e);
                e.printStackTrace();
            }
        }
    }

    public void createGraph3(String beginNode, String endNode, String relType, Map<String, Object> relships) {
        if(!StringUtils.isEmpty(beginNode)  && !StringUtils.isEmpty(endNode)
                && !StringUtils.isEmpty(relType) && null != relships){

            Map<String, Object> params = Maps.newHashMap();
            params.put("beginNode", beginNode);
            params.put("endNode", endNode);
            params.put("reltype", relType);
            params.put("relproperty", relships);

            String cql = "merge (b {name:$beginNode}) \n" +
                    "merge (e {name:$endNode}) \n" +
                    "merge (b)-[r:"+relType+"]-(e) set r={relproperty}";

            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {
                    StatementResult strt = tx.run(cql,params);

//                    Record record = strt.single();
//                    Map resutMap = record.asMap();
//                    System.out.println(resutMap);

                    tx.success();
                } catch (Exception e) {
                    logger.error("createGraph run cql err ", cql, e);
                    e.printStackTrace();
                }
            } catch (Exception e) {
                logger.error("createGraph get session err ", e);
                e.printStackTrace();
            }
        }
    }


    /**
     * mysql数据提取
     * @param jdbcurl
     * @param sql
     */
    public String jdbc(String jdbcurl, String sql, List<String> attRelTypes) {

        String result = null;
        if (StringUtils.isEmpty(jdbcurl)  || StringUtils.isEmpty(sql)  ||  null == attRelTypes ){
            logger.info("neo4j jdbc parameter is empty !");
            return result;
        }

        long begin = System.currentTimeMillis();
        Map<String, Object> params = Maps.newHashMap();
        params.put("sql", sql);
        params.put("jdbcurl", jdbcurl);
        params.put("attRelTypes", attRelTypes);
        //id_no, names, mobile, device_id, GROUP_CONCAT(partner_name) as partner_names
        String cql =  "with {sql} as sql, {jdbcurl} as jdbcurl, {attRelTypes} as attRelTypes \n"+
            "CALL apoc.load.jdbc(jdbcurl,sql) YIELD row \n" +
            "merge (i:ID_NO {id_no:row.id_no, names:split(row.names,',')}) \n" +
            "merge (d:DEVICE {device:row.device_id}) \n" +
            "merge (m:MOBILE {mobile:row.mobile}) \n" +
            "with i, m, d, apoc.coll.toSet(apoc.coll.unionAll(split(row.partner_names,','), attRelTypes)) as relTypes\n" +
            "unwind  relTypes as relType \n" +
            "CALL apoc.merge.relationship(i,relType,{},{},m) yield rel as relIdMobile\n" +
            "CALL apoc.merge.relationship(m,relType,{},{},d) yield rel as relMobileDevice\n" +
            "return count(distinct i) as id_no_count, count(distinct m) as mobile_count, count(distinct d) as device_count, " +
                " count(distinct relIdMobile) as relidmobile_count, count(distinct relMobileDevice) as reliddevice_count\n";


        try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {
                    StatementResult strt = tx.run(cql,Values.value(params));

                    Record record = strt.single();
                    int id_no_count = record.get("id_no_count",0);
                    int mobile_count = record.get("mobile_count",0);
                    int device_count = record.get("device_count",0);
                    int relidmobile_count = record.get("relidmobile_count",0);
                    int reliddevice_count = record.get("reliddevice_count",0);

                    Map<String,Object> resultMap = Maps.newHashMap();
                    resultMap.put("id_no_count", id_no_count);
                    resultMap.put("mobile_count", mobile_count);
                    resultMap.put("device_count", device_count);
                    resultMap.put("relidmobile_count", relidmobile_count);
                    resultMap.put("reliddevice_count", reliddevice_count);
                    result = JSONObject.toJSONString(resultMap);

                    //Map resutMap = record.asMap();
                    System.out.println("id_no_count=" + id_no_count);
                    System.out.println("mobile_count=" + mobile_count);
                    System.out.println("device_count=" + device_count);
                    System.out.println("relidmobile_count=" + relidmobile_count);
                    System.out.println("reliddevice_count=" + reliddevice_count);

                    tx.success();
                } catch (Exception e) {
                    logger.error("createGraph run cql err ", cql, e);
                    e.printStackTrace();
                }
            } catch (Exception e) {
                logger.error("createGraph get session err ", e);
                e.printStackTrace();
            }finally {
                long end = System.currentTimeMillis();
                logger.info("import graph time ={}",end-begin);
            }

            return result;
    }



    /**
     * mysql数据提取
     * @param jdbcurl
     * @param sql
     */
    public void jdbcIte(String jdbcurl, String sql, List<String> attRelTypes) {
        long begin = System.currentTimeMillis();
        Map<String, Object> params = Maps.newHashMap();
        params.put("sql", sql);
        params.put("jdbcurl", jdbcurl);
        params.put("attRelTypes", attRelTypes);

        Map<String, Object> batchs = Maps.newHashMap();
        batchs.put("batchSize", 100);
        batchs.put("parallel", true);
        params.put("batchs", batchs);

        //id_no, names, mobile, device_id, GROUP_CONCAT(partner_name) as partner_names
        String cql =  "with {attRelTypes} as attRelTypes, {batchs} as batchs \n"+
                "CALL apoc.periodic.iterate(\"CALL apoc.load.jdbc('"+jdbcurl+"', '"+sql+"') yield row\", \n" +
                "\"merge (i:ID_NO {id_no:row.id_no, names:split(row.names,',')}) \n" +
                "merge (d:DEVICE {device:row.device_id}) \n" +
                "merge (m:MOBILE {mobile:row.mobile}) \n" +
                "with i, m, d, apoc.coll.toSet(apoc.coll.unionAll(split(row.partner_names,','), {attRelTypes})) as relTypes\n" +
                "unwind  relTypes as relType \n" +
                "CALL apoc.merge.relationship(i,relType,{},{},m) yield rel as relIdMobile\n" +
                "CALL apoc.merge.relationship(m,relType,{},{},d) yield rel as relMobileDevice\", {batchs}) yield batches, total\n" +
                "return  batches, total\n";

        String cql1 =  "with {attRelTypes} as attRelTypes, {batchs} as batchs \n"+
                "CALL apoc.periodic.iterate(\"CALL apoc.load.jdbc('"+jdbcurl+"', '"+sql+"') yield row\", \n" +
                "\"merge (i:ID_NO {id_no:row.id_no}) return i\", {batchs}) yield batches, total \n" +
                "return  batches, total\n";


        try (Session session = driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                StatementResult strt = tx.run(cql,Values.value(params));

                Record record = strt.single();
                int batches = record.get("batches",0);
                int total = record.get("total",0);
                //Map resutMap = record.asMap();
                System.out.println("id_no_count=" + batches);
                System.out.println("total=" + total);

                tx.success();
            } catch (Exception e) {
                logger.error("createGraph run cql err ", cql, e);
                e.printStackTrace();
            }
        } catch (Exception e) {
            logger.error("createGraph get session err ", e);
            e.printStackTrace();
        }finally {
            long end = System.currentTimeMillis();
            logger.info("import graph time ={}",end-begin);
        }
    }




    /**
     * mysql数据提取
     * @param hdfsPath
     */
    public String hdfs(String hdfsPath, String appName, String master) {
        String result = null;
        if (StringUtils.isEmpty(hdfsPath) || StringUtils.isEmpty(appName) || StringUtils.isEmpty(appName)  ){
            logger.info("neo4j hdfs parameter is empty !");
            return result;
        }

        long begin = System.currentTimeMillis();



        Map<String, Object> params = Maps.newHashMap();
        //id_no, names, mobile, device_id, GROUP_CONCAT(partner_name) as partner_names
        String cql =  "with {sql} as sql, {jdbcurl} as jdbcurl, {attRelTypes} as attRelTypes \n"+
                "CALL apoc.load.jdbc(jdbcurl,sql) YIELD row \n" +
                "merge (i:ID_NO {id_no:row.id_no, names:split(row.names,',')}) \n" +
                "merge (d:DEVICE {device:row.device_id}) \n" +
                "merge (m:MOBILE {mobile:row.mobile}) \n" +
                "with i, m, d, apoc.coll.toSet(apoc.coll.unionAll(split(row.partner_names,','), attRelTypes)) as relTypes\n" +
                "unwind  relTypes as relType \n" +
                "CALL apoc.merge.relationship(i,relType,{},{},m) yield rel as relIdMobile\n" +
                "CALL apoc.merge.relationship(m,relType,{},{},d) yield rel as relMobileDevice\n" +
                "return count(distinct i) as id_no_count, count(distinct m) as mobile_count, count(distinct d) as device_count, " +
                " count(distinct relIdMobile) as relidmobile_count, count(distinct relMobileDevice) as reliddevice_count\n";
        Session session = null;
        Transaction tx = null;
        SparkSession spark = null;
        try{
            spark = SparkSession.builder().appName(appName)
                    .master(master)
                    .getOrCreate();

            Dataset<Row> df = spark.read().parquet(hdfsPath);



            //neo.saveGraph();

            session = driver.session();
            tx = session.beginTransaction();
            //StatementResult strt = tx.run(cql,Valueses.value(params));

        }catch (Exception e){
            logger.error("neo4j hdfs run cql err ", cql, e);
            e.printStackTrace();
        }finally {
            if(null != tx){
                tx.close();
            }
            if(null != session){
                session.close();
            }
            if(null != spark){
                spark.close();
            }
            long end = System.currentTimeMillis();
            logger.info("import graph time ={}",end-begin);
        }
        return result;
    }

   ///user/root/hiveDB-dev/ff_platform/interface_logs/get_ad_detail_for_wifi_record/bdp_type=r/bdp_day=20171122

    public static void main(String[] args) throws Exception {

        String url = "bolt://localhost:7687";
        String user = "neo4j";
        String pass = "finup";
        String configPath = "config/neo4j-driver-config.json";

        NeoApocService neoApocDriver = new NeoApocService(configPath);

        String jdbcurl = "jdbc:mysql://10.10.229.152:3306/linkredit?user=linkredit&password=QY2vE0dByHZqORAM";
        String sql = "select id_no, names, mobile, device_id, GROUP_CONCAT(partner_name) as partner_names\n" +
                "from (select id_no, names, mobile, device_id, p.partner_name from\n" +
                "(select id_no, GROUP_CONCAT(distinct name) as names, mobile,partner_id, device_id\n" +
                "from credit_invoke_record where id_no is not null and mobile is not null \n" +
                "and device_id is not null and length(device_id) >0\n" +
                "group by id_no, mobile, partner_id, device_id) t inner join credit_partner p\n" +
                "on t.partner_id = p.id) as t2\n" +
                "group by id_no, names, mobile, device_id";

//        String sql1 = "select idcard, mobile, GROUP_CONCAT(distinct name) as names, GROUP_CONCAT(source) as sources from person\n" +
//                "group by idcard,mobile";

        List<String> attRelType = Arrays.asList("小爱查询");
        neoApocDriver.jdbc(jdbcurl, sql, attRelType);



//        String json = "{'age':123,'name':'ddd'}";
//        Map<String,Object> data = JSON.parseObject(json,Map.class);
//
//
//        System.out.println(data);

    }

}
