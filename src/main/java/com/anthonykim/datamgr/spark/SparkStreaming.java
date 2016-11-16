package com.anthonykim.datamgr.spark;

/**
 * Created by hyuk0 on 2016-11-08.
 */
@Deprecated
public interface SparkStreaming extends Runnable {
    void setTopicNames(String topicNames);

    String getTopicNames();

    void setPartitionAmount(Integer partitionAmount);

    Integer getPartitionAmount();
}
