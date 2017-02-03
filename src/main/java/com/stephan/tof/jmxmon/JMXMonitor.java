package com.stephan.tof.jmxmon;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stephan.tof.jmxmon.HttpClientUtils.HttpResult;
import com.stephan.tof.jmxmon.JVMGCGenInfoExtractor.GCGenInfo;
import com.stephan.tof.jmxmon.JVMMemoryUsedExtractor.MemoryUsedInfo;
import com.stephan.tof.jmxmon.JVMThreadExtractor.ThreadInfo;
import com.stephan.tof.jmxmon.bean.FalconItem;
import com.stephan.tof.jmxmon.bean.JacksonUtil;
import com.stephan.tof.jmxmon.jmxutil.ProxyClient;

public class JMXMonitor {

	private static Logger logger = LoggerFactory.getLogger(JMXMonitor.class);
	private static String pattern = "/services/%s/Pool";
	
	public static void main(String[] args) {
		if (args.length != 1) {
			throw new IllegalArgumentException("Usage: configFile");
		}
		
		try {
			Config.I.init(args[0]);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new IllegalStateException(e);	// 抛出异常便于外部脚本感知
		}

		Map<String, Integer> serviceZkPathToJmxPort = Config.I.getServiceZKPathToJmxPort();
		List<Runnable> tasks = new ArrayList<Runnable>();
		for(String service: serviceZkPathToJmxPort.keySet()) {
			Runnable runnable = new Task(service, serviceZkPathToJmxPort.get(service));
			tasks.add(runnable);
		}

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(tasks.size());
		for(Runnable runnable: tasks) {
			executor.scheduleAtFixedRate(runnable, 0, Config.I.getStep(), TimeUnit.SECONDS);
		}
	}

	/**
	 * 
	 */
	private static void runTask() {
		try {
			List<FalconItem> items = new ArrayList<FalconItem>();
			long start = System.currentTimeMillis();
			int num = 0;
			for (int jmxPort : Config.I.getJmxPorts()) {
				num++;
				// 从JMX中获取JVM信息
				ProxyClient proxyClient = null;
				try {
					proxyClient = ProxyClient.getProxyClient(Config.I.getJmxHost(), jmxPort, null, null);
					proxyClient.connect();
					
					JMXCall<Map<String, GCGenInfo>> gcGenInfoExtractor = new JVMGCGenInfoExtractor(proxyClient, jmxPort, Config.I.getJmxHost());
					Map<String, GCGenInfo> genInfoMap = gcGenInfoExtractor.call();
					items.addAll(gcGenInfoExtractor.build(genInfoMap));
					
					JMXCall<Double> gcThroughputExtractor = new JVMGCThroughputExtractor(proxyClient, jmxPort, Config.I.getJmxHost());
					Double gcThroughput = gcThroughputExtractor.call();
					items.addAll(gcThroughputExtractor.build(gcThroughput));
					
					JMXCall<MemoryUsedInfo> memoryUsedExtractor = new JVMMemoryUsedExtractor(proxyClient, jmxPort, Config.I.getJmxHost());
					MemoryUsedInfo memoryUsedInfo = memoryUsedExtractor.call();
					items.addAll(memoryUsedExtractor.build(memoryUsedInfo));
					
					JMXCall<ThreadInfo> threadExtractor = new JVMThreadExtractor(proxyClient, jmxPort, Config.I.getJmxHost());
					ThreadInfo threadInfo = threadExtractor.call();
					items.addAll(threadExtractor.build(threadInfo));
				} finally {
					if (proxyClient != null) {
						proxyClient.disconnect();
					}
				}
			}

			long cost = System.currentTimeMillis() - start;
			logger.info("Fetch {} JXM info cost {} millis", num, cost);

			// 发送items给Openfalcon agent
			String content = JacksonUtil.writeBeanToString(items, false);
			HttpResult postResult = HttpClientUtils.getInstance().post(Config.I.getAgentPostUrl(), content);
			logger.info("post status=" + postResult.getStatusCode() + 
					", post url=" + Config.I.getAgentPostUrl() + ", content=" + content);
			if (postResult.getStatusCode() != HttpClientUtils.okStatusCode ||
					postResult.getT() != null) {
				throw postResult.getT(); 
			}
			
			// 将context数据回写文件
			Config.I.flush();
		} catch (Throwable e) {
			logger.error(e.getMessage(), e);
		}
	}

	static byte[] parseIP(String ip) {
		String[] ipSecStr = StringUtils.split(ip, ".");
		if (ipSecStr == null || ipSecStr.length < 4) {
			return null;
		}
		byte[] ipSec = new byte[4];
		for (int i = 0; i < 4; i++) {
			ipSec[i] = (byte) Integer.parseInt(ipSecStr[i]);
		}
		return ipSec;
	}

	static class Task implements Runnable {
		private String service;
		private int jmxport;
		private JMXZKNode jmxzkNode;
		private Map<String, String> ipToHost = new HashMap<>();

		public Task(String service, int jmxport) {
			jmxzkNode = new JMXZKNode(Config.I.getZkServers());
			this.service = service;
			this.jmxport = jmxport;
		}

		@Override
		public void run() {
			logger.info("Start collecting jvm counters for service: {}", service);
			long start = System.currentTimeMillis();
			try {
				List<FalconItem> items = new ArrayList<FalconItem>();
				int num = 0;
				List<String> hosts = jmxzkNode.getAllHosts(String.format(pattern, service));
				for (String hostPort : hosts) {
					num++;
					//get ip
					String host = StringUtils.split(hostPort, ":")[0];
					//convert ip to hostname
					if(ipToHost.containsKey(host)) {
						host = ipToHost.get(host);
					} else {
						byte[] ip = parseIP(host);
						try {
							//获得本机的InetAddress信息
							InetAddress IP = InetAddress.getByAddress(ip);
							ipToHost.put(host, IP.getHostName());
							host = IP.getHostName();
						} catch (Exception ex) {
							logger.error("Parse {} to host failed! {}", host, ex);
						}
					}

					// 从JMX中获取JVM信息
					ProxyClient proxyClient = null;
					try {
						proxyClient = ProxyClient.getProxyClient(host, jmxport, null, null);
						proxyClient.connect();

						JMXCall<Map<String, GCGenInfo>> gcGenInfoExtractor = new JVMGCGenInfoExtractor(proxyClient, jmxport, host);
						Map<String, GCGenInfo> genInfoMap = gcGenInfoExtractor.call();
						items.addAll(gcGenInfoExtractor.build(genInfoMap));

						JMXCall<Double> gcThroughputExtractor = new JVMGCThroughputExtractor(proxyClient, jmxport, host);
						Double gcThroughput = gcThroughputExtractor.call();
						items.addAll(gcThroughputExtractor.build(gcThroughput));

						JMXCall<MemoryUsedInfo> memoryUsedExtractor = new JVMMemoryUsedExtractor(proxyClient, jmxport, host);
						MemoryUsedInfo memoryUsedInfo = memoryUsedExtractor.call();
						items.addAll(memoryUsedExtractor.build(memoryUsedInfo));

						JMXCall<ThreadInfo> threadExtractor = new JVMThreadExtractor(proxyClient, jmxport, host);
						ThreadInfo threadInfo = threadExtractor.call();
						items.addAll(threadExtractor.build(threadInfo));
					} finally {
						if (proxyClient != null) {
							proxyClient.disconnect();
						}
					}
				}

				long cost = System.currentTimeMillis() - start;
				logger.info("Fetch {} JXM info cost {} millis", num, cost);

				// 发送items给Openfalcon agent
				String content = JacksonUtil.writeBeanToString(items, false);
				HttpResult postResult = HttpClientUtils.getInstance().post(Config.I.getAgentPostUrl(), content);
				logger.info("post status=" + postResult.getStatusCode() +
						", post url=" + Config.I.getAgentPostUrl() + ", content=" + content);
				if (postResult.getStatusCode() != HttpClientUtils.okStatusCode ||
						postResult.getT() != null) {
					throw postResult.getT();
				}

				// 将context数据回写文件
				Config.I.flush();
			} catch (Throwable e) {
				logger.error(e.getMessage(), e);
			}
			logger.info("Finish collecting jvm counters for service: {}, cost {} millis", service, (System.currentTimeMillis() - start));
		}
	}

}
