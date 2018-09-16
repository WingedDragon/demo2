package com.dw.demo.util;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReflexUtil implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public static <T> T getInstance(Class<?> cls,Class[] clss,Object[] pars) throws Exception{
		return (T)cls.getConstructor(clss).newInstance(pars);
	}
	
	public static <T> T getInstance(Class<?> cls) throws Exception{
		return (T)cls.newInstance();
	}
	
	
	public static Object getInstance(String clsName) throws Exception{
		Object result = null;
		if(!StringUtils.isEmpty(clsName)){
			result = Class.forName(clsName).newInstance();
		}
		return result;
	}

	public static Map<String,Object> getFildKeyValues(Object obj) throws Exception{
		Map<String,Object> fieldKVs = new HashMap<String,Object>();
		if(null != obj){
			Class cls = obj.getClass();
			Method[] methods = cls.getMethods();
			if(null != methods){
				for(Method method : methods){
					String methodName = method.getName();
					if(methodName.startsWith("get") || methodName.startsWith("is") ) {
						String mName = methodName.replace("get","").replace("is","").toLowerCase();
						//String mName = methodName.replace("get","").replace("is","");
						if(!mName.contains("class")){
							Object value = method.invoke(obj);
							//System.out.println(mName+","+value);
							fieldKVs.put(mName,value);
						}

					}
				}
			}
		}
		return fieldKVs;
	}



	public static void setFildKeyValues(Object obj, String fieldName, Object fieldValue) throws Exception{
		if(null != obj){
			Class cls = obj.getClass();
			Method[] methods = cls.getMethods();
			if(null != methods){
				for(Method method : methods){
					String methodName = method.getName();
					if(methodName.startsWith("set") || methodName.startsWith("is") ) {
						String mName = methodName.replace("get","").replace("is","").toLowerCase();
						//String mName = methodName.replace("get","").replace("is","");
						if(!mName.contains("class") && mName.equalsIgnoreCase(fieldName)){

							method.invoke(fieldValue);
						}

					}
				}
			}
		}
	}


	public static List<String> getFilds(Class cls){
		List<String> fieldNames = new ArrayList();
		if(null != cls){
			Method[] methods = cls.getMethods();
			if(null != methods){
				for(Method method : methods){
					String methodName = method.getName();
					if( methodName.startsWith("get") || methodName.startsWith("is") ) {
						//System.out.println(methodName);
						String mName = methodName.replace("get","").replace("is","").toLowerCase();
						if(!mName.contains("class")){
							System.out.println(mName);
							fieldNames.add(mName);
						}
					}
				}
			}
		}
		return fieldNames;
	}
	
	public static void main(String[] args) throws Exception {
		
		Class[] clss = new Class[]{String.class};
		Object[] pars = new Object[]{"my spark"};
//		IWalk walk = (IWalk)ReflexUtil.getInstance(BaseWalk.class, clss, pars);
//		walk.walk();
		
//		IWalk walk2 = (IWalk)getInstance("com.dl.dms.test.BaseWalk");
//		walk2.walk();
		
		//Class udfInterface = Class.forName("org.apache.spark.sql.api.java.UDF1");


		//List<String> fields = getFilds(Employee.class);


//		Employee em = new Employee();
//		Map<String,Object> kvs = getFildKeyValues(em);
		
	}

}
