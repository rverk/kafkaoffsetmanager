package org.ctrlr.kom.daoimplementation;

import com.google.common.io.Files;
import kafka.common.TopicAndPartition;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.ctrlr.kom.testclassification.IntegrationTests;
import org.ctrlr.kom.dao.IOffsetDao;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTests.class)
public class Hbase1OffsetStoreIntegrationTest {

    private static Logger Log = LoggerFactory.getLogger(Hbase1OffsetStoreIntegrationTest.class);

    private HBaseTestingUtility htu;
    private final String tableName = "testKafkaOffsetsTable";
    private final String testGroupID = "testGroupID";
    private final String testTopic = "testTopic";
    private final byte[] colQual = "d".getBytes();
    private final byte[] colFam = "d".getBytes();
    private final String SEPARATOR = "\u0001";
    private IOffsetDao dao;

    @Before
    public void setUp() {

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
    }

    @After
    public void tearDown() {
        try {
            htu.deleteTable(Bytes.toBytes(tableName));
            htu.shutdownMiniHBaseCluster();
            htu.shutdownMiniZKCluster();
            htu.cleanupTestDir();
            dao.close();
            Log.info("Minicluster Shutdown complete");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSetAndGetOffsets() throws Exception {

        assertTrue("No offsets means empty list", dao.getOffsets(testGroupID, testTopic).isEmpty());

        String topic = testTopic;
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
        assertTrue("Offsets have been persisted.", tap.equals(dao.getOffsets(testGroupID, testTopic)));
        assertTrue("But not for other groupid", dao.getOffsets("nonexist", testTopic).isEmpty());
        assertTrue("And not for other topic", dao.getOffsets(testGroupID, "nonexist").isEmpty());
    }



}