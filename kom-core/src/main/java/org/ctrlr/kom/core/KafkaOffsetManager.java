package org.ctrlr.kom.core;

import kafka.common.TopicAndPartition;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.ctrlr.kom.dao.IOffsetDao;
import org.ctrlr.kom.lib.KafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.mutable.ArrayBuffer;
import scala.util.Either;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Manages Kafka offsets for use in Spark Steaming.
 * <p/>
 * Custom kafka offset management is cumbersome. Spark streaming createDirectStream uses
 * the low level/simple kafka API and only provides the ability to start from beginning or end of topic.
 * This is not desireable when processes can stop and start at random or just simple need to be shut down for maintenance.
 * Having the ability to stop and continue where the job left off is therefore essential.
 * <p/>
 * With the help of the KafkaCluster class in spark.streaming interfacing with Kafka has been made easier, but storing
 * all offsets in Zookeeper is providing unneeded pressure on zookeeper and the storing of offsets in a kafka compacted topic
 * has several downsides: 1) lost transparancy, can't read the topic and look at the offsets 2) Kafka API for doing this
 * is unstable at the time of writing. 3) Why use kafka for this when we have a giant highly available, distributed key-value
 * store at your disposal.(hbase) 4) Need plugable storage backend.
 * <p/>
 * Example Usage:
 * final IOffsetStore dao = new Hbase1OffsetStore.Builder()
 * .setHbaseConfiguration(hbaseConfiguration)
 * .setOffsetTable("offsettable").build();
 * <p/>
 * final KafkaOffsetManager osm = new KafkaOffsetManager.Builder()
 * .setOffsetManager(dao)
 * .setKafkaBrokerList(conf.getString("app.kafka.brokers"))
 * .setGroupID(jobUniqueName)
 * .setTopic(conf.getString("app.kafka.topics")).build();
 */
public class KafkaOffsetManager implements AutoCloseable {

    private static Logger Log = LoggerFactory.getLogger(KafkaOffsetManager.class);

    private KafkaCluster kc;
    private String groupid;
    private List<String> topicList = new ArrayList<>();
    private IOffsetDao dao;
    private String startFrom = "earliest";

    private KafkaOffsetManager() {
    }

    public static class Builder {

        private KafkaOffsetManager instance;

        public Builder() {
            instance = new KafkaOffsetManager();
        }

        public Builder setKafkaBrokerList(String brokerList) {
            Map<String, String> kafkaParms = new HashMap<>();
            kafkaParms.put("bootstrap.servers", brokerList);
            instance.kc = new KafkaCluster(ScalaUtils.convertMapToImmutable(kafkaParms));
            return this;
        }

        public Builder setOffsetManager(IOffsetDao dao) {
            instance.dao = dao;
            return this;
        }

        public Builder setGroupID(String groupid) {
            instance.groupid = groupid;
            return this;
        }

        public Builder setTopics(List<String> topics) {
            for (String t : topics) {
                instance.topicList.add(t.trim());
            }
            return this;
        }

        public Builder setTopic(String topic) {
            instance.topicList.add(topic.trim());
            return this;
        }

        public Builder setStartFrom(String startfrom) {
            if (startfrom!=null) {
                instance.startFrom = startfrom.trim().toLowerCase();
            }
            return this;
        }


        /**
         * Do all validation of build parameters
         */
        public KafkaOffsetManager build() {

            String err = "All required parameters were not set: ";
            if (instance.kc == null) {
                throw new IllegalStateException(err + "failed to setup kafkacluster instance, invalid brokerlist?");
            }
            if (StringUtils.isBlank(instance.groupid)) {
                throw new IllegalStateException(err + "Groupid cannot be null or blank.");
            }
            if (instance.topicList.isEmpty()) {
                throw new IllegalStateException(err + "Topic needs to be set.");
            }
            if (instance.dao == null) {
                throw new IllegalStateException(err + "OffsetManagerDAO not set.");
            }
            if (!instance.startFrom.equals("earliest") && !instance.startFrom.equals("latest")) {
                throw new IllegalStateException(err + "StarFrom needs to be either 'earliest' or 'latest'.");
            }

            return instance;
        }
    }

    /**
     * Close up dao on close offsetmanager.
     */
    public void close() {
        if (dao!=null) {
            Log.info("Closing offsetmanager dao.");
            try {
                dao.close();
            } catch (IOException e) {
                Log.warn("OffsetManager Dao close() failed", e);
            }
        }
    }

    /**
     * Returns offset map retrieved from the DAO.
     *
     * @return Map of TopicAndPartitions with their respectfull offsets.
     */
    public Map<TopicAndPartition, Long> getOffsets() {
        Map<TopicAndPartition, Long> returnOffsetsMap = new HashMap<>();

        for (String topic : topicList) {

            Map<TopicAndPartition, Long> resMap = dao.getOffsets(groupid, topic);
            if (!resMap.isEmpty()) {

                Map<TopicAndPartition, Long> earliestMap = getEarliestOffsets();
                for (Map.Entry<TopicAndPartition, Long> entry : earliestMap.entrySet()) {

                    TopicAndPartition currentTAP = entry.getKey();
                    String currentEarliestTopicName = entry.getKey().topic();
                    Long currentEarliestOffset = entry.getValue();

                    /*
                    Using kafka's EarliestOffsetMap, run through all TAPs.
                    1. If tap in stored offsets, and storedOffset>=earliestOffset, add to results
                       else add with earliest offset
                    2. If TAP not in stored offsetMap, add from earliestMap
                     */

                    //stay on topic (lol)
                    if (currentEarliestTopicName.equals(topic)) {

                        if (resMap.containsKey(currentTAP)) {
                            Long currentStoredOffset = resMap.get(currentTAP);
                            if (currentStoredOffset >= currentEarliestOffset) {
                                Log.debug("Found stored partition: {}:{}, adding to map", currentTAP.topic(), currentTAP.partition());
                                returnOffsetsMap.put(currentTAP, currentStoredOffset);
                            } else {
                                Log.info("Stored partition offset lower than earliest, adding as earliest: {}:{}", currentTAP.topic(), currentTAP.partition());
                                Log.info("\tstored:{}, earliest:{}", currentStoredOffset, currentEarliestOffset);
                                returnOffsetsMap.put(currentTAP, currentEarliestOffset);
                            }
                        } else { // Stored offsets dit not contain this TAP at all
                            Log.info("Found new partition: {}:{}, adding to map", currentTAP.topic(), currentTAP.partition());
                            returnOffsetsMap.put(currentTAP, currentEarliestOffset);
                        }
                    }
                }

            } else {
                //There are no offsets for this groupid/topic combo, revert to
                if (startFrom.equals("latest")) {
                    Log.info("No offsets found for {}:{}, reverting to 'latest'", groupid, topic);
                    returnOffsetsMap.putAll(getLatestOffsets(topic));
                } else if (startFrom.equals("earliest")){
                    Log.info("No offsets found for {}:{}, reverting to 'earliest'", groupid, topic);
                    returnOffsetsMap.putAll(getEarliestOffsets(topic));
                } else {
                    //This sould not occur as values are checked in the builder
                    throw new IllegalArgumentException("startFrom cannot be anything other than 'earliest' or 'latest'");
                }
            }
        }
        return returnOffsetsMap;
    }


    /**
     * Sets the offsets in the DAO.
     *
     * @param offsets The map with the offsets per topic+partition. This needs be a valid map,
     *                else an IllegalArgument will be thrown.
     * @throws IllegalArgumentException
     */
    public void setOffsets(Map<TopicAndPartition, Long> offsets) throws IllegalArgumentException {

        if (MapUtils.isEmpty(offsets)) {
            throw new IllegalArgumentException("Offsets map can't be null or empty");
        }
        if (!KafkaOffsetManagerUtils.isValidOffsetMap(offsets)) {
            throw new IllegalArgumentException("Invalid offsets Map<TopicAndPartition, Long>");
        }
        dao.setOffsets(groupid, offsets);
    }

    /**
     * Retrieve the first offsets for all partitions the requested list of topics
     * <p/>
     * Given a topic and groupid, returns a map of TopicPartitions+Offsets to be used as
     * input for the KafkaUtils.createDirectStream method. This method retrieves the earliest possible values
     * at the beginning of the topic.
     *
     * @return a map of TopicPartitions-Offsets
     */
    public Map<TopicAndPartition, Long> getEarliestOffsets() {

        Map<TopicAndPartition, Long> returnOffsetsMap = new HashMap<>();

        for (String topic : topicList) {
            returnOffsetsMap.putAll(getEarliestOffsets(topic));
        }

        return returnOffsetsMap;
    }


    /**
     * Retrieve the first offsets for all partitions in a kafka topic.
     * <p/>
     * Given a topic and groupid, returns a map of TopicPartitions+Offsets to be used as
     * input for the KafkaUtils.createDirectStream method. This method retrieves the earliest possible values
     * at the beginning of the topic.
     *
     * @return a map of TopicPartitions-Offsets
     */
    public Map<TopicAndPartition, Long> getEarliestOffsets(String topic) {

        Map<TopicAndPartition, Long> returnOffsetsMap = new HashMap<>();

        Log.info("Getting earliest offsets for, topic: '{}'", topic);

        scala.collection.immutable.Set<TopicAndPartition> topicPartitionSet = getPartitionsForTopcic(topic);
        Either<ArrayBuffer<Throwable>, scala.collection.immutable.Map<TopicAndPartition, KafkaCluster.LeaderOffset>> results = kc.getEarliestLeaderOffsets(topicPartitionSet);


        if (results.isRight()) {

            for (Map.Entry<TopicAndPartition, KafkaCluster.LeaderOffset> entry : JavaConverters.asJavaMapConverter(results.right().get()).asJava().entrySet()) {
                returnOffsetsMap.put(new TopicAndPartition(entry.getKey().topic(), entry.getKey().partition()), entry.getValue().offset());
            }

        } else {
            Log.warn("Problem getting Offsets for this topic and groupid: {}", topic);
        }
        return returnOffsetsMap;
    }

    /**
     * Retrieve the latest available offsets for all partitions of the requested topiclist
     * <p/>
     * Given a topic and groupid, returns a map of TopicPartitions-Offsets to be used as
     * input for the KafkaUtils.createDirectStream method. This method retrieves the earliest possible values
     * at the beginning of the topic.
     *
     * @return a map of TopicPartitions-Offsets
     */
    public Map<TopicAndPartition, Long> getLatestOffsets() {

        Map<TopicAndPartition, Long> returnOffsetsMap = new HashMap<>();
        for (String topic : topicList) {
            returnOffsetsMap.putAll(getLatestOffsets(topic));
        }
        return returnOffsetsMap;
    }

    /**
     * Retrieve the latest available offsets for all partitions in a kafka topic.
     * <p/>
     * Given a topic and groupid, returns a map of TopicPartitions-Offsets to be used as
     * input for the KafkaUtils.createDirectStream method. This method retrieves the earliest possible values
     * at the beginning of the topic.
     *
     * @return a map of TopicPartitions-Offsets
     */
    public Map<TopicAndPartition, Long> getLatestOffsets(String topic) {

        Map<TopicAndPartition, Long> returnOffsetsMap = new HashMap<>();


        Log.debug("Getting latest leader offsets for, topic: '{}'", topic);

        scala.collection.immutable.Set<TopicAndPartition> topicPartitionSet = getPartitionsForTopcic(topic);
        Either<ArrayBuffer<Throwable>, scala.collection.immutable.Map<TopicAndPartition, KafkaCluster.LeaderOffset>> results = kc.getLatestLeaderOffsets(topicPartitionSet);


        if (results.isRight()) {

            for (Map.Entry<TopicAndPartition, KafkaCluster.LeaderOffset> entry : JavaConverters.asJavaMapConverter(results.right().get()).asJava().entrySet()) {
                returnOffsetsMap.put(new TopicAndPartition(entry.getKey().topic(), entry.getKey().partition()), entry.getValue().offset());
            }

        } else {
            Log.warn("Problem getting Offsets for this topic and groupid: {}, returning empty map", topic);
        }
        return returnOffsetsMap;
    }


    protected scala.collection.immutable.Set<TopicAndPartition> getPartitionsForTopcic(String topic) {

        assert (StringUtils.isNotBlank(topic));
        Log.debug("Getting Partitions for topic: {}", topic);

        Set<String> topicSet = new HashSet<>(Collections.singletonList(topic));
        scala.collection.immutable.Set<String> topicSetScala = JavaConverters.asScalaSetConverter(topicSet).asScala().toSet();
        Either<ArrayBuffer<Throwable>, scala.collection.immutable.Set<TopicAndPartition>> results = kc.getPartitions(topicSetScala);

        if (results.isRight()) {
            return results.right().get();
        } else {
            Log.warn("Problem getting PartitionsForTopic, returning empty map.");
            return new scala.collection.immutable.HashSet<>();
        }
    }
}


