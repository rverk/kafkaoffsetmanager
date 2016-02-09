package org.ctrlr.kom.core;

import com.github.charithe.kafka.KafkaJunitRule;
import kafka.common.TopicAndPartition;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.ctrlr.kom.dao.IOffsetDao;
import org.ctrlr.kom.dao.StubOffsetStore;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.immutable.Set;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.*;

public class KafkaOffsetManagerTest {

    private static Logger Log = LoggerFactory.getLogger(KafkaOffsetManagerTest.class);
    private KafkaOffsetManager validKOM;

    private IOffsetDao dao;
    private String testTopicName = "testTopic";

    @ClassRule
    public static KafkaJunitRule kafkaRule = new KafkaJunitRule();

    @Before
    public void setUp() throws Exception {

        dao = new StubOffsetStore();

        validKOM = new KafkaOffsetManager.Builder()
                .setOffsetManager(dao)
                .setKafkaBrokerList("localhost:" + kafkaRule.kafkaBrokerPort())
                .setGroupID("testGroupID")
                .setTopic(testTopicName).build();
    }

    @Test
    public void integrationTestKafkaOffsetManager() throws TimeoutException {

        // Need these to run in this order:
        testEmptyListOnEmptyKafkaTopic();
        testProduceAMessage();
        testGettingOffsets();
        testGetPartitionsForTopcic();

    }

    private void testEmptyListOnEmptyKafkaTopic() {

        assertTrue("Return empty list, not null", validKOM.getEarliestOffsets().isEmpty());
        assertTrue("Return empty list, not null", validKOM.getLatestOffsets().isEmpty());
    }

    private void testProduceAMessage() throws TimeoutException {

        // Produce a message so we can check new offsets.
        ProducerConfig conf = kafkaRule.producerConfigWithStringEncoder();
        Producer<String, String> producer = new Producer<>(conf);
        producer.send(new KeyedMessage<>(testTopicName, "key", "value"));
        producer.close();

        // Verify publish
        List<String> messages = kafkaRule.readStringMessages(testTopicName, 1);
        assertThat(messages, is(notNullValue()));
        assertThat(messages.size(), is(1));
        assertThat(messages.get(0), is("value"));
    }

    private void testGettingOffsets() {

        Map<TopicAndPartition, Long> tap = new HashMap<>();
        tap.put(new TopicAndPartition(testTopicName, 0), 0L);
        assertTrue("After publish, ealiest is 0:0", validKOM.getEarliestOffsets().equals(tap));

        Map<TopicAndPartition, Long> tap2 = new HashMap<>();
        tap2.put(new TopicAndPartition(testTopicName, 0), 1L);
        assertTrue("After publish, latest is 0:1", validKOM.getLatestOffsets().equals(tap2));
    }

    private void testGetPartitionsForTopcic() {

        Set<TopicAndPartition> test = validKOM.getPartitionsForTopcic(testTopicName);
        assertEquals(1, test.size());

        for (TopicAndPartition tap : JavaConverters.asJavaSetConverter(test).asJava()) {
            assertEquals(testTopicName, tap.topic());
            assertTrue(tap.partition() == 0);
        }
    }

    @Test
    public void testSetOffSetsOK() throws Exception {

        Map<TopicAndPartition, Long> tap = new HashMap<>();
        tap.put(new TopicAndPartition(testTopicName, 1), 1L);
        tap.put(new TopicAndPartition(testTopicName, 2), 2L);

        validKOM.setOffsets(tap);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetOffsetsWithInvalidParam() throws Exception {

        Map<TopicAndPartition, Long> tap = new HashMap<>();
        validKOM.setOffsets(tap);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetWithInvalidParams() throws Exception {

        validKOM.setOffsets(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetOffSetsWrongMap() throws Exception {

        Map<TopicAndPartition, Long> tap = new HashMap<>();
        tap.put(new TopicAndPartition(testTopicName, 1), 1L);
        tap.put(new TopicAndPartition(testTopicName+"1", 2), 2L);

        validKOM.setOffsets(tap);
    }

}