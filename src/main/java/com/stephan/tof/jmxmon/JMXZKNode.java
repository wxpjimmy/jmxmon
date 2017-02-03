package com.stephan.tof.jmxmon;

import org.I0Itec.zkclient.ZkClient;

import java.util.List;

/**
 * Created by wxp04 on 2017/2/3.
 */
public class JMXZKNode {
    private int defaultSessionTimeout = 30000;
    private int defaultConnectionTimeout = 30000;
    private ZkClient zkClient;

    public JMXZKNode(String zkservers) {
        zkClient = new ZkClient(zkservers, defaultSessionTimeout, defaultConnectionTimeout);
    }

    public List<String> getAllHosts(String zkPath) {
        return zkClient.getChildren(zkPath);
    }

    public void close() {
        zkClient.close();
    }
}
