package com.stephan.tof.jmxmon;

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
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
	private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
	
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


		String jmxZkConfigPath = Config.I.getJmxZKConfigPath();
	 	JMXZKNode jmxzkNode = new JMXZKNode(Config.I.getZkServers(), new ZKStringSerializer());
		List<Runnable> tasks = new ArrayList<Runnable>();

		if (StringUtils.isBlank(jmxZkConfigPath)) {
			Map<String, Integer> serviceZkPathToJmxPort = Config.I.getServiceZKPathToJmxPort();
			for (String service : serviceZkPathToJmxPort.keySet()) {
				Runnable runnable = new Task(service, serviceZkPathToJmxPort.get(service));
				tasks.add(runnable);
			}
		} else  {
			String data = jmxzkNode.getContent(jmxZkConfigPath);
			String[] parts = StringUtils.split(data, ",");
			for(String part:parts) {
				String[] secs = StringUtils.split(part, ":");
				String zkServicePath = secs[0];
				int jmxPort = Integer.parseInt(secs[1]);
				String serviceTag = null;
				if (secs.length > 2) {
					serviceTag = secs[2];
				}
				Runnable runnable = new Task(jmxzkNode, zkServicePath, jmxPort, serviceTag);
				tasks.add(runnable);
			}
		}

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(tasks.size());
		for(Runnable runnable: tasks) {
			executor.scheduleAtFixedRate(runnable, 0, Config.I.getStep(), TimeUnit.SECONDS);
		}
	}

	static class ZKStringSerializer implements ZkSerializer {

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			return ((String) data).getBytes(DEFAULT_CHARSET);
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			return new String(bytes, DEFAULT_CHARSET);
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
		private String serviceZKPath;
		private int jmxport;
		private JMXZKNode jmxzkNode;
		//tag used in counter
		private String serviceTag;
		private Map<String, String> ipToHost = new HashMap<>();

		@Deprecated
		private Task(String serviceZKPath, int jmxport) {
			jmxzkNode = new JMXZKNode(Config.I.getZkServers());
			this.serviceZKPath = serviceZKPath;
			this.jmxport = jmxport;
		}

		public Task(JMXZKNode jmxzkNode, String serviceZkPath, int jmxport) {
			this(jmxzkNode, serviceZkPath, jmxport, null);
		}

		public Task(JMXZKNode jmxzkNode, String serviceZkPath, int jmxport, String serviceName) {
			this.jmxzkNode = jmxzkNode;
			this.serviceZKPath = serviceZkPath;
			this.jmxport = jmxport;
			this.serviceTag = serviceName;
		}

		@Override
		public void run() {
			logger.info("Start collecting jvm counters for serviceZKPath: {}", serviceZKPath);
			long start = System.currentTimeMillis();
			try {
				List<FalconItem> items = new ArrayList<FalconItem>();
				int num = 0;
				List<String> hosts = jmxzkNode.getAllHosts(serviceZKPath);
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

						JMXCall<Map<String, GCGenInfo>> gcGenInfoExtractor = new JVMGCGenInfoExtractor(proxyClient, jmxport, host, serviceTag);
						Map<String, GCGenInfo> genInfoMap = gcGenInfoExtractor.call();
						items.addAll(gcGenInfoExtractor.build(genInfoMap));

						JMXCall<Double> gcThroughputExtractor = new JVMGCThroughputExtractor(proxyClient, jmxport, host, serviceTag);
						Double gcThroughput = gcThroughputExtractor.call();
						items.addAll(gcThroughputExtractor.build(gcThroughput));

						JMXCall<MemoryUsedInfo> memoryUsedExtractor = new JVMMemoryUsedExtractor(proxyClient, jmxport, host, serviceTag);
						MemoryUsedInfo memoryUsedInfo = memoryUsedExtractor.call();
						items.addAll(memoryUsedExtractor.build(memoryUsedInfo));

						JMXCall<ThreadInfo> threadExtractor = new JVMThreadExtractor(proxyClient, jmxport, host, serviceTag);
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
			logger.info("Finish collecting jvm counters for serviceZKPath: {}, cost {} millis", serviceZKPath, (System.currentTimeMillis() - start));
		}
	}

}
