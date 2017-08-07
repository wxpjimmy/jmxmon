package com.stephan.tof.jmxmon;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.util.List;

/**
 * Created by wangxiaopeng on 2017/2/3.
 */
public class JMXZKNode {
    private int defaultSessionTimeout = 30000;
    private int defaultConnectionTimeout = 30000;
    private ZkClient zkClient;

    public JMXZKNode(String zkservers) {
        zkClient = new ZkClient(zkservers, defaultSessionTimeout, defaultConnectionTimeout);
    }

    public JMXZKNode(String zkservers, ZkSerializer zkSerializer) {
        zkClient = new ZkClient(zkservers, defaultSessionTimeout, defaultConnectionTimeout, zkSerializer);
    }

    public List<String> getAllHosts(String zkPath) {
        return zkClient.getChildren(zkPath);
    }

    public String getContent(String zkPath) {
        return zkClient.readData(zkPath, true);
    }

    public void close() {
        zkClient.close();
    }
}
