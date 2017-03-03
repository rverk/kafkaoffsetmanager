package org.ctrlr.kom.core;

import kafka.common.TopicAndPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class TestUtils {

    private static Logger Log = LoggerFactory.getLogger(TestUtils.class);

    public static void printTaP(Map<TopicAndPartition, Long> topicAndPartitionLongMap) {
        for (Map.Entry<TopicAndPartition, Long> entry : topicAndPartitionLongMap.entrySet()) {
            Log.info("{}:{}, {}", entry.getKey().topic(), entry.getKey().partition(), entry.getValue());
        }
    }
}
