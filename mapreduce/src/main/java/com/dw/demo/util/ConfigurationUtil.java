package com.dw.demo.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 配置文件工具类
 */
public class ConfigurationUtil {

	private final static Logger log = LoggerFactory.getLogger(ConfigurationUtil.class);


	/**
	 * 从classpath中获取配置文件
	 * @return
     */
	public static Configuration getHadoopConf4ClassPath() {
		Configuration conf = new Configuration();
		try {
			//conf = HBaseConfiguration.create();
			conf.addResource(ConfigurationUtil.class.getClassLoader().getResourceAsStream("core-site.xml"));
			conf.addResource(ConfigurationUtil.class.getClassLoader().getResourceAsStream("hdfs-site.xml"));
			//conf.addResource(ConfigurationUtil.class.getClassLoader().getResourceAsStream("singleconf/hbase-site.xml"));
			conf.addResource(ConfigurationUtil.class.getClassLoader().getResourceAsStream("mapred-site.xml"));
			conf.addResource(ConfigurationUtil.class.getClassLoader().getResourceAsStream("yarn-site.xml"));
		}catch (Exception e) {
			e.printStackTrace();
		}
		return conf;
	}
	
	


	public static String getHadoopConfValue(String key,Configuration conf ) {
		String value = null;
		if(null != conf && !StringUtils.isEmpty(key)){
			value = conf.get(key);
		}
		return value;
	}


	public static void main(String[] args) {
		
		Configuration conf = getHadoopConf4ClassPath();
		
		System.out.println(conf.get("fs.defaultFS"));

	}

}
