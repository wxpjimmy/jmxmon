package com.stephan.tof.jmxmon;

import com.stephan.tof.jmxmon.bean.FalconItem;
import com.stephan.tof.jmxmon.jmxutil.ProxyClient;

import java.io.IOException;
import com.stephan.tof.jmxmon.JVMBufferUsedExtractor.BufferPoolUsedInfo;
import org.apache.commons.lang.StringUtils;

import java.lang.management.BufferPoolMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by liushufan on 2018/8/13.
 */
public class JVMBufferUsedExtractor extends JVMDataExtractor<Map<String, BufferPoolUsedInfo>> {
    private String host;

    public JVMBufferUsedExtractor(ProxyClient proxyClient, int jmxPort, String host, String serviceTag) throws IOException{
        super(proxyClient, jmxPort, serviceTag);
        this.host = host;
    }

    @Override
    public Map<String, BufferPoolUsedInfo> call() throws Exception {
        Map<String, BufferPoolUsedInfo> result = new HashMap<>();

        for(BufferPoolMXBean bufferPoolMXBean: getBufferPoolList()){
            BufferPoolUsedInfo bufferPoolUsedInfo = new BufferPoolUsedInfo(bufferPoolMXBean.getCount(), bufferPoolMXBean.getMemoryUsed());
            result.put(bufferPoolMXBean.getName(), bufferPoolUsedInfo);

            logger.debug("mxbean=" + bufferPoolMXBean.getName() +
                    ", host=" + host + ", jmxport=" + getJmxPort() +
                    ", niobuffer count=" + bufferPoolUsedInfo.getBufferCount() +
                    ", niobuffer total memory used =" + bufferPoolUsedInfo.getMemoryUsed());
        }

        return result;
    }

    @Override
    public List<FalconItem> build(Map<String, BufferPoolUsedInfo> jmxResultData) throws Exception {
        List<FalconItem> items = new ArrayList<FalconItem>();

        StringBuilder tagsBuilder = new StringBuilder();
        tagsBuilder.append("jmxport=").append(getJmxPort());
        if(StringUtils.isNotBlank(getServiceTag())) {
            tagsBuilder.append(",").append("service=").append(getServiceTag());
        }
        String tags = StringUtils.lowerCase(tagsBuilder.toString());

        for(String bfMxBeanName:jmxResultData.keySet()){
            FalconItem countItem = new FalconItem();

            countItem.setCounterType(Constants.CounterType.GAUGE.toString());
            countItem.setEndpoint(host);
            countItem.setMetric(StringUtils.lowerCase(bfMxBeanName + Constants.metricSeparator + Constants.nioBufferCount));
            countItem.setStep(Constants.defaultStep);
            countItem.setTags(tags);
            countItem.setTimestamp(System.currentTimeMillis() / 1000);
            countItem.setValue(jmxResultData.get(bfMxBeanName).getBufferCount());
            items.add(countItem);

            FalconItem nioMemUsedItem = new FalconItem();
            nioMemUsedItem.setCounterType(Constants.CounterType.GAUGE.toString());
            nioMemUsedItem.setEndpoint(host);
            nioMemUsedItem.setMetric(StringUtils.lowerCase(bfMxBeanName + Constants.metricSeparator + Constants.nioBufferMemused));
            nioMemUsedItem.setStep(Constants.defaultStep);
            nioMemUsedItem.setTags(tags);
            nioMemUsedItem.setTimestamp(System.currentTimeMillis() / 1000);
            nioMemUsedItem.setValue(jmxResultData.get(bfMxBeanName).getMemoryUsed());
            items.add(nioMemUsedItem);
        }

        return items;
    }

    class BufferPoolUsedInfo{
        private final long bufferCount;
        private final long memoryUsed;

        public BufferPoolUsedInfo(long bufferCount, long memoryUsed){
            this.bufferCount = bufferCount;
            this.memoryUsed = memoryUsed;
        }

        /**
         * Returns an estimate of the number of buffers in the pool.
         *
         * @return  An estimate of the number of buffers in this pool
         */
        public long getBufferCount() {
            return bufferCount;
        }

        /**
         * Returns an estimate of the memory that the Java virtual machine is using
         * for this buffer pool. The value returned by this method may differ
         * from the estimate of the total {@link # BufferPoolMXBean.getTotalCapacity capacity} of
         * the buffers in this pool. This difference is explained by alignment,
         * memory allocator, and other implementation specific reasons.
         *
         * @return  An estimate of the memory that the Java virtual machine is using
         *          for this buffer pool in bytes, or {@code -1L} if an estimate of
         *          the memory usage is not available
         */
        public long getMemoryUsed() {
            return memoryUsed;
        }
    }
}
