package org.ctrlr.kom.daoimplementation;

import com.google.common.base.Joiner;
import kafka.common.TopicAndPartition;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.ctrlr.kom.dao.IOffsetDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hbase 1.0 Storage backend implementation for KafkaOffsetManager
 * <p/>
 * Uses Hbase 1.0 Client API implementation.
 * <p/>
 * * Create the hbase table as follows:
 * <p>
 * $ create '[table_name]', { NAME => 'd', VERSIONS => 1, COMPRESSION => 'SNAPPY'}
 * </p>
 * <p>
 * Example Usage:
 * final Configuration hbaseConfiguration = HBaseConfiguration.create();
 * hbaseConfiguration.addResource(new Path(str));
 * <p/>
 * final IOffsetStore dao = new Hbase1OffsetStore.Builder()
 * .setHbaseConfiguration(hbaseConfiguration)
 * .setOffsetTable("offsettable").build();
 * </p>
 */
public class Hbase1OffsetStore implements IOffsetDao {

    private static Logger Log = LoggerFactory.getLogger(Hbase1OffsetStore.class);
    private Connection conn;
    private String offsetTable;

    private final String SEPARATOR = "\u0001";
    private final byte[] colFam = "d".getBytes();
    private final byte[] colQual = "d".getBytes();

    private Hbase1OffsetStore() {
    }

    public static class Builder {

        private Hbase1OffsetStore instance;

        public Builder() {
            instance = new Hbase1OffsetStore();
        }

        public Builder setOffsetTable(String table) {
            instance.offsetTable = table;
            return this;
        }

        public Builder setHbaseConfiguration(Configuration hconfig) {
            try {
                instance.conn = ConnectionFactory.createConnection(hconfig);
            } catch (IOException e) {
                Log.error("Failed to create a hbase connection", e);
                instance.conn = null;
            }
            return this;
        }


        /**
         * Do all validation of build parameters
         */
        public Hbase1OffsetStore build() {

            String err = "All required parameters were not set: ";
            if (StringUtils.isBlank(instance.offsetTable)) {
                throw new IllegalStateException(err + "Offsettable needs to be set.");
            }
            if (instance.conn == null) {
                throw new IllegalStateException(err + "HbaseConfiguration not set or invalid.");
            }

            return instance;
        }
    }

    @Override
    /**
     * Get the stored offsets for a given groupid on a topic.
     * <p>
     * Given a topic and groupid, returns a map of TopicPartitions->Offsets to be used as
     * input for the KafkaUtils.createDirectStream method.
     *
     * @return a map of TopicPartitions->Offsets
     * @throws UnexpectedException
     */
    public Map<TopicAndPartition, Long> getOffsets(String groupid, String topic) {

        Map<TopicAndPartition, Long> returnOffsetsMap = new HashMap<>();
        Log.info("Getting hbase consumer offsets for, topic: '{}', groupid: '{}'", topic, groupid);

        try {
            Table htable = conn.getTable(TableName.valueOf(offsetTable));
            byte[] startKey = Bytes.toBytes(Joiner.on(SEPARATOR)
                    .join(groupid, topic, ""));
            Scan scan = new Scan(startKey);
            scan.setConsistency(Consistency.TIMELINE);

            /* Set prefix filter */
            PrefixFilter pfFilter = new PrefixFilter(startKey);
            scan.setFilter(pfFilter);

            try (ResultScanner rs = htable.getScanner(scan)) {
                for (Result r : rs) {
                    String key = Bytes.toString(r.getRow());

                    String hgroupid = key.split(SEPARATOR)[0];
                    String htopic = key.split(SEPARATOR)[1];
                    String partition = key.split(SEPARATOR)[2];

                    Long offset = Bytes.toLong(r.getValue(colFam, colQual));
                    Log.info("Found hbase offsets for groupid:{} topic:{}, {}:{}", hgroupid, htopic, partition, offset);

                    returnOffsetsMap.put(new TopicAndPartition(topic, Integer.parseInt(partition)), offset);
                }
            }

            htable.close();
        } catch (IOException e) {
            Log.warn("Problem getting Offsets for this topic: {} and groupid: {}, returning empty list.", topic, groupid);
        }

        return returnOffsetsMap;
    }

    @Override
    /**
     * Stores offsets to hbase.
     * <p>
     * Use this function in a forEach spark step where the offsets are iterated.
     *
     * @param offsets The map of T&P and offsets to store.
     */
    public void setOffsets(String groupid, Map<TopicAndPartition, Long> offsets) {

        if (conn == null || conn.isClosed()) {
            throw new IllegalArgumentException("Connection is null or closed.");
        }

        try {
            BufferedMutator bm = conn.getBufferedMutator(TableName.valueOf(offsetTable));

            for (Map.Entry<TopicAndPartition, Long> entry : offsets.entrySet()) {
                String key = Joiner.on(SEPARATOR).join(groupid, entry.getKey().topic(), entry.getKey().partition());

                Put put = new Put(key.getBytes());
                put.addColumn(colFam, colQual, Bytes.toBytes(entry.getValue()));
                bm.mutate(put);
            }

            bm.flush();
            bm.close();

        } catch (IOException e) {
            Log.warn("Failed to write offsets to hbase:", e);
        }
    }

    @Override
    public void close() {
        try {
            Log.info("Closing dao connection.");
            conn.close();
        } catch (IOException ignored) {  }
    }

}
