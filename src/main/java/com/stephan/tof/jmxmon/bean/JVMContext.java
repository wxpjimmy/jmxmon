package com.stephan.tof.jmxmon.bean;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 用来缓存通过JMX获取到的JVM数据，每次程序退出时需要将此对象序列化到文件中，以便下次启动时能够再次加载到内存中使用
 * 
 * @author Stephan gao
 * @since 2016年4月26日
 *
 */
public class JVMContext {

	/**
	 * port -> jvm data
	 */
	@JsonProperty
	private Map<String, JVMData> jvmDatas = new LinkedHashMap<String, JVMData>();

	public JVMData getJvmData(Integer jmxPort, String host) {
		String key = host + ":" + jmxPort;
		if(jvmDatas.containsKey(key)) {
			return jvmDatas.get(key);
		} else {
			JVMData jvmData = new JVMData();
			jvmDatas.put(key, jvmData);
			return jvmData;
		}
	}
	
}
