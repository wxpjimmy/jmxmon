package com.stephan.tof.jmxmon.zookeeper;

import com.stephan.tof.jmxmon.Config;
import com.stephan.tof.jmxmon.JMXMonitor;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by wangxiaopeng on 2017/2/3.
 */
public class JMXZKManager {
    private static Logger logger = LoggerFactory.getLogger(JMXZKManager.class);

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    private int defaultSessionTimeout = 30000;
    private int defaultConnectionTimeout = 30000;
    private ZkClient zkClient;
    private String zkConfigPath;
    private ConcurrentMap<String, JMXZKConfigItem> configItems;
    private ScheduledExecutorService executorService;

    public JMXZKManager(String zkservers) {
        zkClient = new ZkClient(zkservers, defaultSessionTimeout, defaultConnectionTimeout, new ZKStringSerializer());
        this.zkConfigPath = Config.I.getJmxZKConfigPath();
        this.configItems = new ConcurrentHashMap<String, JMXZKConfigItem>();
        if (StringUtils.isNotBlank(zkConfigPath)) {
            String data = getContent(zkConfigPath);
            this.configItems.putAll(parseJmxZKConfig(data));
            if (zkClient.exists(zkConfigPath)) {
                zkClient.subscribeDataChanges(zkConfigPath, new IZkDataListener() {
                    @Override
                    public void handleDataChange(String dataPath, Object data) throws Exception {
                        Map<String, JMXZKConfigItem> temp = parseJmxZKConfig((String) data);
                        if (temp.size() > 0) {
                            for (String key : temp.keySet()) {
                                if (!configItems.containsKey(key)) {
                                    logger.info("Found newly added config path: {}", key);
                                    configItems.put(key, temp.get(key));
                                    addNewTask(key);
                                }
                            }
                            Set<String> removed = new HashSet<String>();
                            for (String key : configItems.keySet()) {
                                if (!temp.containsKey(key)) {
                                    logger.info("Found removed config path: {}", key);
                                    removed.add(key);
                                }
                            }
                            for (String toRemove : removed) {
                                configItems.remove(toRemove);
                            }
                        }
                    }

                    @Override
                    public void handleDataDeleted(String dataPath) throws Exception {
                        if (configItems != null) {
                            configItems.clear();
                        }
                    }
                });
            }
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

    private void addNewTask(String serviceNodeZKPath) {
        Runnable newTask = new JMXMonitor.Task(this, serviceNodeZKPath);
        executorService.scheduleAtFixedRate(newTask, 0, Config.I.getStep(), TimeUnit.SECONDS);
    }

    private Map<String, JMXZKConfigItem> parseJmxZKConfig(String config) {
        Map<String, JMXZKConfigItem> temp = new HashMap<String, JMXZKConfigItem>();
        try {
            String[] parts = StringUtils.split(config, ",");
            for (String part : parts) {
                String[] secs = StringUtils.split(part, ":");
                String zkNodeServicePath = secs[0];
                int jmxPort = Integer.parseInt(secs[1]);
                String serviceTag = null;
                if (secs.length > 2) {
                    serviceTag = secs[2];
                }
                temp.put(zkNodeServicePath, new JMXZKConfigItem(zkNodeServicePath, jmxPort, serviceTag));
            }
        } catch (Exception ex) {
            logger.error("Parse Jmx ZKConfig failed!", ex);
        }
        return temp;
    }

    public void setExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.executorService = scheduledExecutorService;
    }

    public List<String> getAllHosts(String zkPath) {
        return zkClient.getChildren(zkPath);
    }

    private String getContent(String zkPath) {
        return zkClient.readData(zkPath, true);
    }

    public Set<String> getJmxZKConfigPaths() {
        return configItems.keySet();
    }

    public JMXZKConfigItem getConfigItem(String serviceNodeZKPath) {
        return configItems.get(serviceNodeZKPath);
    }

    public void close() {
        zkClient.close();
    }
}
