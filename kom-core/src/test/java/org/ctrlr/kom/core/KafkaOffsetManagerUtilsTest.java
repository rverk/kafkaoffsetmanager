package org.ctrlr.kom.core;

import kafka.common.TopicAndPartition;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class KafkaOffsetManagerUtilsTest {

    private final String testTopic = "testTopic";

    @Test
    public void testMapOK() throws Exception {

        // Create TAP, Long map to test with
        Map<TopicAndPartition, Long> tap = new HashMap<>();
        tap.put(new TopicAndPartition(testTopic, 1), 1L);
        tap.put(new TopicAndPartition(testTopic, 2), 2L);

        assertTrue(KafkaOffsetManagerUtils.isValidOffsetMap(tap));
    }

    @Test
    public void testMapWithBelowZeroOffset() throws Exception {

        // Create TAP, Long map to test with
        Map<TopicAndPartition, Long> tap = new HashMap<>();
        tap.put(new TopicAndPartition(testTopic, 1), 1L);
        tap.put(new TopicAndPartition(testTopic, 2), -2L);

        assertFalse(KafkaOffsetManagerUtils.isValidOffsetMap(tap));
    }

    @Test
    public void testMapWithMultipleTopics() throws Exception {

        Map<TopicAndPartition, Long> tap = new HashMap<>();
        tap.put(new TopicAndPartition(testTopic, 1), 1L);
        tap.put(new TopicAndPartition(testTopic + "1", 2), 2L);

        assertFalse(KafkaOffsetManagerUtils.isValidOffsetMap(tap));
    }

    @Test
    public void testMapWithPartitionBelowZero() throws Exception {

        // Create TAP, Long map to test with
        Map<TopicAndPartition, Long> tap = new HashMap<>();
        tap.put(new TopicAndPartition(testTopic, -1), 1L);

        assertFalse(KafkaOffsetManagerUtils.isValidOffsetMap(tap));
    }
}