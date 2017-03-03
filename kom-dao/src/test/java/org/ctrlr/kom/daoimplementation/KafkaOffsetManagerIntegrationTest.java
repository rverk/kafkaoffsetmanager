package org.ctrlr.kom.daoimplementation;

import com.github.charithe.kafka.KafkaJunitRule;
import com.google.common.io.Files;
import kafka.common.TopicAndPartition;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.ctrlr.kom.core.KafkaOffsetManager;
import org.ctrlr.kom.dao.IOffsetDao;
import org.ctrlr.kom.testclassification.IntegrationTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTests.class)
public class KafkaOffsetManagerIntegrationTest {

    private static Logger Log = LoggerFactory.getLogger(KafkaOffsetManagerIntegrationTest.class);
    private KafkaOffsetManager KOM;

    private HBaseTestingUtility htu;
    private final String tableName = "testKafkaOffsetsTable";
    private final String testGroupID = "testGroupID";
    private final byte[] colQual = "d".getBytes();
    private final byte[] colFam = "d".getBytes();
    private final String SEPARATOR = "\u0001";
    private IOffsetDao dao;
    private String testTopicName = "testTopic";

    @ClassRule
    public static KafkaJunitRule kafkaRule = new KafkaJunitRule();

    @Before
    public void setUp() throws Exception {

        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();

        htu = HBaseTestingUtility.createLocalHTU();
        try {
            htu.cleanupTestDir();

            htu.startMiniZKCluster();
            htu.startMiniHBaseCluster(1, 1);

            try {
                htu.deleteTable(Bytes.toBytes(tableName));
            } catch (Exception e) {
                Log.info(" - no table " + tableName + " found");
            }
            htu.createTable(Bytes.toBytes(tableName), colFam);

            dao = new Hbase1OffsetStore.Builder()
                    .setHbaseConfiguration(htu.getConfiguration())
                    .setOffsetTable(tableName).build();

        } catch (Exception e1) {
            throw new RuntimeException(e1);
        }

        KOM = new KafkaOffsetManager.Builder()
                .setOffsetManager(dao)
                .setKafkaBrokerList("localhost:" + kafkaRule.kafkaBrokerPort())
                .setGroupID(testGroupID)
                .setTopic(testTopicName).build();
    }

    @After
    public void tearDown() {
        try {
            htu.deleteTable(Bytes.toBytes(tableName));
            htu.shutdownMiniHBaseCluster();
            htu.shutdownMiniZKCluster();
            htu.cleanupTestDir();
            Log.info("Minicluster Shutdown complete");
            KOM.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void integrationTestKafkaOffsetManager() throws TimeoutException, ExecutionException, InterruptedException {

        testProduceAMessage();
        KOM.setOffsets(KOM.getLatestOffsets());
        testProduceAMessage();

        Map<TopicAndPartition, Long> tap = new HashMap<>();
        tap.put(new TopicAndPartition(testTopicName, 0), 0L);
        assertTrue("Earliest is 0:0", KOM.getEarliestOffsets().equals(tap));

        tap.clear();
        tap.put(new TopicAndPartition(testTopicName, 0), 1L);
        assertTrue("Persisted to store is 0:1", KOM.getOffsets().equals(tap));

        tap.clear();
        tap.put(new TopicAndPartition(testTopicName, 0), 2L);
        assertTrue("Latest is 0:2", KOM.getLatestOffsets().equals(tap));
    }

    private void testProduceAMessage() throws TimeoutException, ExecutionException, InterruptedException {

        // Produce a message so we can check new offsets.
        KafkaProducer<String, String> producer = kafkaRule.createStringProducer();
        producer.send(new ProducerRecord<>(testTopicName, "key", "value"));
        producer.close();

        // Verify publish
        List<ConsumerRecord<String, String>> messages = kafkaRule.pollStringMessages(testTopicName, 1).get(5, TimeUnit.SECONDS);
        assertThat(messages, is(notNullValue()));
        assertThat(messages.size(), is(1));

        ConsumerRecord<String, String> msg = messages.get(0);
        assertThat(msg, is(notNullValue()));
        assertThat(msg.value(), is("value"));
    }

    @Test
    public void testNewPartitions() throws Exception {

        assertTrue("No offsets means empty list", dao.getOffsets(testGroupID, testTopicName).isEmpty());

        String topic = testTopicName;
        // Create TAP, Long map to test with
        Map<TopicAndPartition, Long> tap = new HashMap<>();
        tap.put(new TopicAndPartition(topic, 1), 1L);
        tap.put(new TopicAndPartition(topic, 2), 2L);

        // Let the dao write these to hbase
        dao.setOffsets(testGroupID, tap);

        // Check expected table and key of the offset storage
        Connection connection = ConnectionFactory.createConnection(htu.getConfiguration());
        Table table = connection.getTable(TableName.valueOf(tableName));

        final String prefix = new StringBuilder().append(testGroupID).append(SEPARATOR).append(topic).append(SEPARATOR).toString();
        assertEquals(1L,
                Bytes.toLong(
                        CellUtil.cloneValue(
                                table.get(
                                        new Get(Bytes.toBytes(prefix + "1")))
                                        .getColumnLatestCell(colFam, colQual))));

        assertEquals(2L,
                Bytes.toLong(
                        CellUtil.cloneValue(
                                table.get(
                                        new Get(Bytes.toBytes(prefix + "2")))
                                        .getColumnLatestCell(colFam, colQual))));
        assertTrue("Offsets have been persisted.", tap.equals(dao.getOffsets(testGroupID, testTopicName)));
        assertTrue("But not for other groupid", dao.getOffsets("nonexist", testTopicName).isEmpty());
        assertTrue("And not for other topic", dao.getOffsets(testGroupID, "nonexist").isEmpty());
    }
}