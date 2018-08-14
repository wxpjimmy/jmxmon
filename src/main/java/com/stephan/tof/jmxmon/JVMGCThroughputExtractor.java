package com.stephan.tof.jmxmon;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.stephan.tof.jmxmon.Constants.CounterType;
import com.stephan.tof.jmxmon.bean.FalconItem;
import com.stephan.tof.jmxmon.jmxutil.ProxyClient;

public class JVMGCThroughputExtractor extends JVMDataExtractor<Double> {
	private String host;

	public JVMGCThroughputExtractor(ProxyClient proxyClient, int jmxPort, String host, String serviceTag)
			throws IOException {
		super(proxyClient, jmxPort, serviceTag);
		this.host = host;
	}

	@Override
	public Double call() throws Exception {
		RuntimeMXBean rbean = getRuntimeMXBean();
		
		long upTime = rbean.getUptime();
		long totalGCTime = 0;
		Collection<GarbageCollectorMXBean> list = getGcMXBeanList();
		for (GarbageCollectorMXBean bean : list) {
			totalGCTime += bean.getCollectionTime();
		}
		
		double gcThroughput = (double) (upTime - totalGCTime) * 100 / (double) upTime;
		return gcThroughput;
	}

	@Override
	public List<FalconItem> build(Double jmxResultData) throws Exception {
		List<FalconItem> items = new ArrayList<FalconItem>();

		try {
			// 将jvm信息封装成openfalcon格式数据
			FalconItem item = new FalconItem();
			item.setCounterType(CounterType.GAUGE.toString());
			item.setEndpoint(host);
			item.setMetric(StringUtils.lowerCase(Constants.gcThroughput));
			item.setStep(Constants.defaultStep);
			StringBuilder tagsBuilder = new StringBuilder();
			tagsBuilder.append("jmxport=").append(getJmxPort());
			if (StringUtils.isNotBlank(getServiceTag())) {
				tagsBuilder.append(",").append("service=").append(getServiceTag());
			}
			item.setTags(StringUtils.lowerCase(tagsBuilder.toString()));
			item.setTimestamp(System.currentTimeMillis() / 1000);
			item.setValue(jmxResultData);

			items.add(item);
		} catch (Exception e){
			logger.error("fail to generate gc throughput", e);
		}
		
		return items;
	}

}
