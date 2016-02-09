package org.ctrlr.kom.dao;

import kafka.common.TopicAndPartition;

import java.util.Map;

/**
 * Interface to implement for adding an external OffsetDaoImplementation
 */
public interface IOffsetDao {

    Map<TopicAndPartition, Long> getOffsets(String groupid, String topic);

    void setOffsets(String groupid, Map<TopicAndPartition, Long> offsets);
}
