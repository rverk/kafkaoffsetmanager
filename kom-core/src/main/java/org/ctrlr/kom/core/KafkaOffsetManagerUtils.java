package org.ctrlr.kom.core;

import kafka.common.TopicAndPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Util function(s) for the Manager
 */
class KafkaOffsetManagerUtils {

    private static Logger Log = LoggerFactory.getLogger(KafkaOffsetManagerUtils.class);

    static boolean isValidOffsetMap(Map<TopicAndPartition, Long> offsets) {

        assert(offsets!=null);

        Set<String> topicSet = new HashSet<>();

        for (Map.Entry<TopicAndPartition, Long> entry : offsets.entrySet()) {
            TopicAndPartition tap = entry.getKey();
            topicSet.add(tap.topic());

            if (tap.partition()<0) {
                Log.error("partition must be >=0");
                return false;
            }

            Long offset = entry.getValue();
            if (offset<0L) {
                Log.error("invalid offset map. offset needs to be >=0");
                return false; }
        }

        if (topicSet.size()!=1) {
            Log.error("One and only one topic allowed in a TopicAndPartion map");
            return false;
        }

        return true;
    }
}
