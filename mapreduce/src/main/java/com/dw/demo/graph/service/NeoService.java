package com.dw.demo.graph.service;

import com.alibaba.fastjson.JSONObject;
import com.dw.demo.graph.config.NeoServerConfig;
import com.dw.demo.graph.constant.NeoConstant;
import com.dw.demo.util.CommonUtil;
import com.dw.demo.util.JsonObjParser;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.xml.internal.xsom.impl.scd.Iterators;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.mortbay.util.ajax.JSON;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.v1.*;
import org.slf4j.LoggerFactory;
import scala.math.Ordering;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by finup on 2018/7/25.
 */
public class NeoService {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(NeoApocService.class);


    //配置
    private NeoServerConfig neoConfig;

    //连接驱动
    private Driver driver;


    /**
     * neo4j 连接服务
     * @param configPath
     */
    public NeoService(String configPath) {
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


    /**
     * 连接关闭
     * @throws Exception
     */
    public void close() throws Exception {
        if (null != driver) {
            driver.close();
        }
    }

    /**
     * 连接驱动
     * @return
     */
    public Driver getDriver() {
        return driver;
    }




    /**
     * 创建/修改 节点
     * @param cql     Arrays.asList("A","B")
     * @param parameters properties.put("name","1");   properties.put("sex","man");
     */
    public Boolean writeNode(String cql,  Map<String, Object> parameters) {
        Boolean result = false;
        if(StringUtils.isNotEmpty(cql) && MapUtils.isNotEmpty(parameters)){
            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {
                    StatementResult strt = tx.run(cql, Values.value(parameters));
                    Record record = strt.single();
                    tx.success();
                    result = true;
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
     * 删除节点
     * @param cql
     * @param parameters
     * @return
     */
    public Boolean removeNode(String cql,  Map<String, Object> parameters) {
        Boolean result = false;
        if(StringUtils.isNotEmpty(cql) && MapUtils.isNotEmpty(parameters)){
            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {
                    tx.run(cql, Values.value(parameters));
                    tx.success();
                    result = true;
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
     * @param cql
     * @param parameters
     */
    public Boolean createNodes(String cql, Map<String, Object> parameters) {
        Boolean result = false;
        if(StringUtils.isNotEmpty(cql) && StringUtils.isNotEmpty(cql) ){
            List<Map<String,Object>> nodes = Lists.newArrayList();
            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {

                    StatementResult strt = tx.run(cql, Values.value(parameters));
                    List<Record> records = strt.list();
                    for(Record record : records){
                        nodes.add(record.asMap());
                    }
                    tx.success();

                    System.out.println("neo4j.createNodes=" + JSONObject.toJSONString(nodes));
                    result = true;
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
     * 查询节点
     * @param cql
     * @param parameters
     * @return
     */
    public List<Record> findNode(String cql,Map<String, Object> parameters) {
        List<Record> records = Lists.newArrayList();
        if(!StringUtils.isEmpty(cql)){
            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {
                    StatementResult strt = tx.run(cql, Values.value(parameters));
                    records = strt.list();
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
        return records;

    }




    /**
     * 创建关系
     */
    public Long addRelationship(String cql, Map<String, Object> parameters) {
        Long relID = null;
        if(StringUtils.isNotBlank(cql) && null != parameters){

            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {

                    StatementResult strt = tx.run(cql, Values.value(parameters));
//                    List<Record> records = strt.list();
//                    for(Record record : records){
//                        String json = JSON.toString(record.asMap());
//                        System.out.println(json);
//                    }
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
     * 查询路径
     * @param cql
     * @param parameters
     * @return
     */
    public List<Record> findPath(String cql,Map<String, Object> parameters) {
        List<Record> records = Lists.newArrayList();
        if(!StringUtils.isEmpty(cql)){
            try (Session session = driver.session()) {
                try (Transaction tx = session.beginTransaction()) {
                    StatementResult strt = tx.run(cql, Values.value(parameters));
                    records = strt.list();
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
        return records;

    }



    //======================================================================


    /**
     * 测试单点创建
     * @param neoService
     */
    public static void testCreateNode(NeoService neoService) {

        //1 普通创建节点
        Map<String, Object> params = new HashedMap();
        params.put("name", "haha");
        params.put("age", 100);

        String cql = "CREATE (e:Employee { name: {name}, age: {age}}) return e ";
        neoService.writeNode(cql, params);

        //2 动态属性
        Map<String, Object> properties = new HashedMap();
        properties.put("name", "a");
        properties.put("age", 1);

        Map<String, Object> params2 = new HashedMap();
        params2.put("params", properties);

        List<String> labels = Arrays.asList("E1","E2", "E3");
        String cql2 = "CREATE (p:Person {params}) return p ";
        //neoService.writeNode(cql2,  params2);
    }

    /**
     * 测试单点创建
     * @param neoService
     */
    public static void testUpdateNode(NeoService neoService) {
        //修改属性
        Map<String, Object> properties = new HashedMap();
        properties.put("name", "ac-999");
        properties.put("newname", "ac-999");
        properties.put("newage", 999);
        properties.put("newsex", "man");

        List<String> labels = Arrays.asList("E1","E2", "E3");

        //修改属性
        String cql = "match (e:Employee) where e.name = {name} set e.name = {newname}, e.age = {newage}  return e ";

        //增加属性
        String cql2 = "match (e:Employee) where e.name = {name} set e.sex = {newsex} return e ";

        //删除属性
        String cql3 = "match (e:Employee) where e.name = {name} set e.sex = NULL return e ";


        neoService.writeNode(cql3,  properties);
    }

    /**
     * 测试单点
     * @param neoService
     */
    public static void testRemoveNode(NeoService neoService) {
        //修改属性
        Map<String, Object> properties = new HashedMap();
        properties.put("name", "haha");

        //删除
        String cql = "match (e:Employee) where e.name = {name}  delete e ";


        neoService.removeNode(cql,  properties);
    }



    /**
     * 测试多点创建
     * @param neoService
     */
    public static void testCreateNodes4Person(NeoService neoService) {

        //1 多节点
        List<Map<String, Object>> nodeParas = Lists.newArrayList();
        String def_name = "p";
        for(int i=1; i<10; i++){
            String name = def_name + "_" + i;
            Map<String, Object> nodePara = new HashedMap();
            nodePara.put("name", name);
            nodePara.put("age", 10 * i);

            nodeParas.add(nodePara);
        }

        Map<String, Object> parameters = new HashedMap();
        parameters.put("persons",nodeParas);

        String cql = "UNWIND {persons} AS person CREATE (p:Person) set p.name = person.name, p.age = person.age return p";
        neoService.createNodes(cql, parameters);
    }


    /**
     * 测试多点创建
     * @param neoService
     */
    public static void testCreateNodes4Phone(NeoService neoService) {

        //1 多节点
        List<Map<String, Object>> nodeParas = Lists.newArrayList();
        String def_name = "p";
        for(int i=1; i<10; i++){
            String mobile = CommonUtil.getRandom(4);
            Map<String, Object> nodePara = new HashedMap();
            nodePara.put("mobile", mobile);

            nodeParas.add(nodePara);
        }

        Map<String, Object> parameters = new HashedMap();
        parameters.put("phones",nodeParas);

        String cql = "UNWIND {phones} AS phone CREATE (p:Phone) set p.mobile = phone.mobile return p";
        neoService.createNodes(cql, parameters);
    }

    /**
     * 测试查询1：节点
     * @param neoService
     */
    public static void testFindNodes(NeoService neoService) {

        Map<String, Object> parameters = new HashedMap();
        parameters.put("name","ac-999");
        parameters.put("label","Employee");

        //1 精确匹配
        String cql0 = "match (n:Employee) where n.name = 'ac-999' return n";
        String cql1 = "match (n:Employee) where n.name = $name return n";
        String cql11 = "match (n) where n:Employee return n";

        //2 模糊匹配
        String cql21 = "match (n:Person) where n.name =~ '(?i)p_1.*' return n"; //.操作符号（正则开头）
        String cql22 = "match (n:Person) where n.name ends with '_6' return n";
        String cql23 = "match (n:Person) where n.name starts with 'p_' return n";
        String cql24 = "match (n:Person) where n['age'] > 60 return n";

        //3 范围匹配
        String cql3 = "match (n:Person) where n.name IN ['p_1','p_2','p_3'] return n";

        List<Record> records = neoService.findNode(cql11, parameters);
        for(Record record : records){
            List<String> keys = record.keys();
            System.out.println("keys=" + JSONObject.toJSONString(keys));

            Map<String,Object> values = record.get("n", Maps.newHashMap());
            System.out.println("values=" + JSONObject.toJSONString(values));
        }
    }


    /**
     * 测试查询2:节点和关系
     * @param neoService
     */
    public static void testFindNodesAndRels(NeoService neoService) {

        Map<String, Object> parameters = new HashedMap();
        parameters.put("pname","p_1");

        //3 范围匹配
        String cql = "match (n:Person)-[r]->(m:Phone) where n.name = $pname return n, m";

        List<Record> records = neoService.findNode(cql, parameters);
        for(Record record : records){
            List<String> keys = record.keys();
            System.out.println("keys=" + JSONObject.toJSONString(keys));
            for(String key : keys){
                Object v = record.get(key).asObject();
                System.out.println("key=" + key +",values=" + JSONObject.toJSONString(v));
            }
        }
    }


    /**
     * 测试查询2:节点和关系
     * @param neoService
     */
    public static void testFindNodesAndRels2(NeoService neoService) {
        for(int i=1; i<10; i++){
            String cql = "match (n:Person),(m:Person) " +
                    " where n.name = $name1 and m.name = $name2 " +
                    " merge (n)-[r:Friend]->(m) return n,m";


            String name1 = "p_"+i;
            String name2 = "p_"+CommonUtil.getRandomNumStr(1);

            Map<String, Object> parameters = new HashedMap();
            parameters.put("name1", name1);
            parameters.put("name2", name2);

            List<Record> records = neoService.findNode(cql, parameters);
            for(Record record : records){
                List<String> keys = record.keys();
                System.out.println("keys=" + JSONObject.toJSONString(keys));
                for(String key : keys){
                    Object v = record.get(key).asObject();
                    System.out.println("key=" + key +",values=" + JSONObject.toJSONString(v));
                }
            }

        }
    }

    /**
     * 关系
     * @param neoService
     */
    public static void testRelation(NeoService neoService) {

        Map<String, Object> parameters = new HashedMap();

        parameters.put("ename","p_1");
        //parameters.put("mobile","je4c");

        //关系（属性）
        parameters.put("owner","yes");
        parameters.put("ct","2018-07-01");
        parameters.put("mobile","7je6");


        String cql = " match (e:Person),(p:Phone)" +
                " where e.name = $ename and p.mobile = $mobile " +
                " merge (e)-[r:OwnerPhones {owner:$owner,ct:$ct}]->(p)" +
                " return e, r, p ";


        neoService.addRelationship(cql, parameters);

    }


    /**
     * 路径
     * @param neoService
     */
    public static void testPath(NeoService neoService) {

        Map<String, Object> parameters = new HashedMap();

        parameters.put("nname","p_3");
        parameters.put("mname","p_1");
        //parameters.put("mobile","je4c");

        //路径
        String cql1 = " match (n:Person),(m:Person) where n.name = $nname " +
                " match graph=(n)<-[r:Friend*1..3]-(m) " +
                " return n, length(r) as plen, m";

        //最短路径(shortestPath)
        String cql2 = " match (n:Person),(m:Person) where n.name = $nname and m.name=$mname " +
                " match shortPath=shortestPath((n)-[*1..8]->(m)) " +
                " return n,m,shortPath";

        //MATCH (P1 {name : "P1"}),(P2 {name : "P2"}) MATCH p=shortestPath(P1-[*..15]->P2) return P1,P2,p;


        List<Record> records = neoService.findPath(cql2, parameters);
        for(Record record : records){
            Map<String,Object> keyvalues = record.asMap();
            for(Iterator<Map.Entry<String,Object>> ite = keyvalues.entrySet().iterator(); ite.hasNext();){
                Map.Entry<String,Object> entry = ite.next();
                String key = entry.getKey();
                Object value = entry.getValue();
                System.out.println("key="+key+",value="+ value.toString());
            }

        }

    }

    public static void main(String[] args) throws Exception {

        String url = "bolt://localhost:7687";
        String user = "neo4j";
        String pass = "finup";
        String configPath = "graph/neo4j-driver-config.json";

        NeoService neoService = new NeoService(configPath);

        //1 单节点创建
        //testCreateNode(neoService);

        //2 修改节点
        //testUpdateNode(neoService);

        //3 删除节点
        //testRemoveNode(neoService);

        //4 多节点创建
        //testCreateNodes4Person(neoService);
        //testCreateNodes4Phone(neoService);

        //5 创建关系
        //testRelation(neoService);
        //testFindNodesAndRels2(neoService);

        //6 查询测试
        //testFindNodes(neoService);
        //testFindNodesAndRels(neoService);

        //7 路径关系
        //testPath(neoService);





    }


}
