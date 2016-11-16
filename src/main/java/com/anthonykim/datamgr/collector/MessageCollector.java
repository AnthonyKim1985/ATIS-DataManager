package com.anthonykim.datamgr.collector;

/**
 * Created by hyuk0 on 2016-10-27.
 */
public interface MessageCollector extends Runnable {
    Long PARTITION_STATUS_CHECK_CYCLE = 60000L;

    void setTopicName(String topicName);

    String getTopicName();

    void setPartitionAmount(Integer partitionAmount);

    Integer getPartitionAmount();
}