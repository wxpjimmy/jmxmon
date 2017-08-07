package com.stephan.tof.jmxmon.zookeeper;

/**
 * Created by wangxiaopeng on 2017/8/7.
 */
public class JMXZKConfigItem {
    private String serviceNodeZKPath;
    private int jmxPort;
    private String serviceTag;

    public JMXZKConfigItem(String serviceNodeZKPath, int jmxPort, String serviceTag) {
        this.serviceNodeZKPath = serviceNodeZKPath;
        this.jmxPort = jmxPort;
        this.serviceTag = serviceTag;
    }

    public String getServiceNodeZKPath() {
        return serviceNodeZKPath;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public String getServiceTag() {
        return serviceTag;
    }
}
