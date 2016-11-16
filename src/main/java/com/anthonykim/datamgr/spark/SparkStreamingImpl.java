package com.anthonykim.datamgr.spark;

import com.anthonykim.datamgr.common.PropertiesUtil;
import kafka.serializer.StringDecoder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by hyuk0 on 2016-11-08.
 */
@Log4j
@NoArgsConstructor
@AllArgsConstructor
@Deprecated
public class SparkStreamingImpl implements SparkStreaming {
    private String topicNames;
    private Integer partitionAmount;

    private static SparkConf sparkConf;
    private static JavaSparkContext sparkContext;
    private static JavaStreamingContext streamingContext;
    private static Map<String, String> kafkaParams;

    static {
        Properties sparkProps = PropertiesUtil.getExternalProperties("conf/spark-config.properties");
        if (sparkProps == null)
            sparkProps = PropertiesUtil.getDefaultProperties("META-INF/properties/default-spark-config.properties");

        sparkConf = new SparkConf().setAppName("spark-streaming").setMaster(sparkProps.getProperty("spark.uri"));
        sparkContext = new JavaSparkContext(sparkConf);
        streamingContext = new JavaStreamingContext(sparkContext, new Duration(Long.parseLong(sparkProps.getProperty("stream.duration.ms"))));

        Properties kafkaProps = PropertiesUtil.getExternalProperties("conf/kafka-streaming.properties");
        if (kafkaProps == null)
            kafkaProps = PropertiesUtil.getDefaultProperties("META-INF/properties/default-kafka-streaming.properties");

        kafkaParams = new HashMap<>();
        for (Object key : kafkaProps.keySet())
            kafkaParams.put((String) key, (String) kafkaProps.get(key));
    }

    public void setTopicNames(String topicNames) {
        this.topicNames = topicNames;
    }

    public String getTopicNames() {
        return topicNames;
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
        Set<String> topicSet = new HashSet<>(Arrays.asList(topicNames.split("[,]")));
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(streamingContext,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicSet);

        directKafkaStream.foreachRDD((VoidFunction<JavaPairRDD<String, String>>) stringStringJavaPairRDD -> {
            System.out.println("--- New RDD with " + stringStringJavaPairRDD.partitions().size() + " partitions and " + stringStringJavaPairRDD.count() + " records");
            stringStringJavaPairRDD.foreach((VoidFunction<Tuple2<String, String>>) stringStringTuple2 -> System.out.println(stringStringTuple2._1 + "\t:\t" + stringStringTuple2._2));
        });
        directKafkaStream.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}