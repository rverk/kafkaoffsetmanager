package org.ctrlr.kom.dao;

import kafka.common.TopicAndPartition;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * Interface to implement for adding an external OffsetDaoImplementation
 */
public interface IOffsetDao extends Closeable {

    Map<TopicAndPartition, Long> getOffsets(String groupid, String topics);

    void setOffsets(String groupid, Map<TopicAndPartition, Long> offsets);

    //void close();
}
