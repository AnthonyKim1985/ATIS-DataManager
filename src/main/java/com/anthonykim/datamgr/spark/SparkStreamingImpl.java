package com.anthonykim.datamgr.spark;

import com.anthonykim.datamgr.common.PropertiesUtil;

import kafka.serializer.StringDecoder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;

/**
 * Created by hyuk0 on 2016-11-08.
 */
@Log4j
@NoArgsConstructor
@AllArgsConstructor
@Deprecated
public class SparkStreamingImpl implements SparkStreaming {
    private String topicName;
    private Integer partitionAmount;

    private static JavaSparkContext sparkContext;
    private static JavaStreamingContext streamingContext;
    private static Map<String, String> kafkaParams;

    static {
        Properties sparkProps = PropertiesUtil.getExternalProperties("conf/spark-config.properties");
        if (sparkProps == null)
            sparkProps = PropertiesUtil.getDefaultProperties("META-INF/properties/default-spark-config.properties");

        SparkConf sparkConf = new SparkConf().setAppName("datamgr-spark-streaming").setMaster(sparkProps.getProperty("spark.uri"));
        sparkContext = new JavaSparkContext(sparkConf);
        streamingContext = new JavaStreamingContext(sparkContext, new Duration(1000));

        Properties kafkaProps = PropertiesUtil.getExternalProperties("conf/kafka-streaming.properties");
        if (kafkaProps == null)
            kafkaProps = PropertiesUtil.getDefaultProperties("META-INF/properties/default-kafka-streaming.properties");

        kafkaParams = new HashMap<>();
        for (Object key : kafkaProps.keySet())
            kafkaParams.put((String) key, (String) kafkaProps.get(key));
    }

    @Override
    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public String getTopicName() {
        return topicName;
    }

    @Override
    public void setPartitionAmount(Integer partitionAmount) {
        this.partitionAmount = partitionAmount;
    }

    @Override
    public Integer getPartitionAmount() {
        return partitionAmount;
    }

    @Override
    public void run() {
        Set<String> topics = Collections.singleton(topicName);
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(streamingContext,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            log.info("--- New RDD with " + rdd.partitions().size() + " partitions and " + rdd.count() + " records");
            rdd.foreach(record -> log.info(record._2));
        });
    }
}
