package com.online.demo.util.constant;

import com.online.demo.enumeration.BusyDBEnum;

import java.io.Serializable;

/**
 * Created by Administrator on 2017-6-9.
 */
public class ReleaseConstant implements Serializable{

    //===kafka-topic====================================================================
    public static final String TOPIC_TEST = "dltest"; //测试通道

    //===es====================================================================
    public static final String ES_TEMPLATE_REALEASE_BIDDING = "releasetemplate";//投放竞价模版
    public static final String ES_INDEX_ALIASNAME_REALEASE_BIDDING = "release_bidding";//投放竞价

    public static final String ES_RESOURCE = "es.resource";
    public static final String ES_RESOURCE_READ = "es.resource.read";
    public static final String ES_RESOURCE_WRITE = "es.resource.write";

    public static final String ES_RESOURCE_READ_ALLVALUES = "_all/types";


    //===hbase====================================================================
    public static final String HBASE_BLOCK_CACHEENABLED = "hbase.cf.block.cache.enabled";
    public static final String HBASE_MAXVERSIONS = "hbase.cf.version.max";
    public static final String HBASE_MINVERSIONS = "hbase.cf.version.min";
    public static final String HBASE_BLOCK_SIZE = "hbase.cf.block.size";
    public static final String HBASE_CF_NAME = "hbase.cf.name";
    public static final String HBASE_CLIENT_WRITE_BUFFER = "hbase.client.write.buffer";
    public static final String HBASE_BATCH_SIZE="hbase.bacth.size";


    public static final String HBASE_NAMESPACE = "xx"; //名称空间

    public static final String HBASE_TABLE_xxx = "tbl_xxx";


    public static final String PHOENIX_DRIVER = "phoenix.driver";
    public static final String PHOENIX_ZK_QUORUM = "phoenix.zk.quorum";

    //===zk====================================================================
    public static final String ZK_CONNECT = "zk.connect";
    public static final String ZK_CONNECT_KAFKA = "zk.kafka.connect";
    public static final String ZK_SESSION_TIMEOUT = "zk.session.timeout";
    public static final String ZK_CONN_TIMEOUT = "zk.connection.timeout";
    public static final String ZK_BEE_ROOT = "zk.bee.root";


    //===spark dataset schema====================================================================
    public static final String SPARK_SCHEMA_ENTITY = "entity"; //entity
    public static final String SPARK_SCHEMA_FIELDS = "fields"; //fields
    public static final String SPARK_SCHEMA_FIELD_NAME = "name"; //name
    public static final String SPARK_SCHEMA_FIELD_DATATYPE = "datatype"; //datatype
    public static final String SPARK_SCHEMA_FIELD_NULLABLE = "nullable"; //nullable


    //===Field DataType===========================
    public static final String DATATYPE_BOOLEAN = "Boolean";
    public static final String DATATYPE_BYTE = "Byte";
    public static final String DATATYPE_INT = "Int";
    public static final String DATATYPE_LONG = "Long";
    public static final String DATATYPE_DOUBLE = "Double";
    public static final String DATATYPE_FLOAT = "Float";
    public static final String DATATYPE_STRING = "String";
    public static final String DATATYPE_DATE = "Date";//"0001-01-01" through "9999-12-31".
    public static final String DATATYPE_DECIMAL = "Decimal"; //Decimal
    public static final String DATATYPE_TIMESTAMP = "Timestamp";//Timestamp

    public static final String DATATYPE_ARRAY = "Array";//Array
    public static final String DATATYPE_MAP = "Map";//Map
    public static final String DATATYPE_STRUCT = "Struct";//Struct
    public static final String DATATYPE_DEF = "String";


    //---投放环节-------------------------------------------------------------
    public static final String RELEASE_NOTCUSTOMER = "00";
    public static final String RELEASE_CUSTOMER = "01";
    public static final String RELEASE_BIDDING = "02";
    public static final String RELEASE_SHOW = "03";
    public static final String RELEASE_CLICK = "04";
    public static final String RELEASE_ARRIVE = "05";
    public static final String RELEASE_REGISTER = "06";


    //---元数据信息--------------------------------------------------------------------------
    public static final String DB_TABLE = "dbtable";
    public static final String BIDDING_COUNT = "bidding_count";

    public static final String DEVICENUM = "deviceNum";
    public static final String DEVICENUM_TYPE = "deviceNumType";
    public static final String ENCRPT_DEVICENUM = "encryptDeviceNum";
    public static final String SESSION_ID = "sessionId";
    public static final String SOURCE = "source";
    public static final String STATUS = "status";
    public static final String TARGET = "target";
    public static final String CT = "ct";
    public static final String UT = "ut";
    public static final String PLAN_NO = "planNo";
    public static final String EXTMAP = "extMap";
    public static final String BDP_DAY = "bdp_day";

    public static final String COUNT = "count";

    public static final String MATTER_TYPE = "matterType";
    public static final String MODEL_CODE = "modelCode";
    public static final String MODEL_VERSION = "modelVersion";
    public static final String MODEL_RULE_CODE = "modelRuleCode";
    public static final String CATEGORY = "category";
    public static final String BANNER_TYPE = "bannerType";
    public static final String SID = "sid";


    public static final String REQUEST_ID = "requestId";
    public static final String PRICE = "price";
    public static final String PRICING_TYPE = "pricingType";
    public static final String BIDDING_TYPE = "biddingType";

    //============================================================

    public static final String DBTABLE_BIDDING = BusyDBEnum.REALEASE_BIDDING.getDb() + ReleaseConstant.BOTTOM_LINE + BusyDBEnum.REALEASE_BIDDING.getTable();



    //===常用符号====================================================================

    public static final String Encoding_UTF8 = "UTF-8";
    public static final String Encoding_GBK = "GBK";

    public static final String MIDDLE_LINE = "-";
    public static final String BOTTOM_LINE = "_";
    public static final String COMMA = ",";
    public static final String SEMICOLON = ";";
    public static final String PLINE = "|";
    public static final String COLON = ":";
    public static final String PATH_W = "\\";
    public static final String PATH_L = "/";
    public static final String POINT = ".";
    public static final String BLANK = " ";

    public static final String LEFT_ARROWS = "<-";
    public static final String RIGHT_ARROWS = "->";

    public static final String LEFT_BRACKET = "[";
    public static final String RIGHT_BRACKET = "]";

    public static final String TAB = "\t";


}
