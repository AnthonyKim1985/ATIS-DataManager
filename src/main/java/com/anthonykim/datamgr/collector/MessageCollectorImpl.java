package com.anthonykim.datamgr.collector;

import com.anthonykim.datamgr.common.PropertiesUtil;
import com.anthonykim.datamgr.distributor.MessageDistributor;
import com.anthonykim.datamgr.distributor.MessageDistributorImpl;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by hyuk0 on 2016-10-27.
 */
@Log4j
@NoArgsConstructor
@AllArgsConstructor
public class MessageCollectorImpl implements MessageCollector {
    private String topicName;
    private Integer partitionAmount;

    private static Properties kafkaProps;

    static {
        kafkaProps = PropertiesUtil.getExternalProperties("conf/kafka-collector.properties");
        if (kafkaProps == null)
            kafkaProps = PropertiesUtil.getDefaultProperties("META-INF/properties/default-kafka-collector.properties");
    }

    @Override
    public void run() {
        log.info("Message Distributor " + topicName + " started with amount of partitions " + partitionAmount + "...");

        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProps);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topicName, partitionAmount);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        final List<KafkaStream<byte[], byte[]>> kafkaStreamList = consumerMap.get(topicName);
        final List<WeakReference<Thread>> streamThreadList = new LinkedList<>();

        for (int i = 0; i < kafkaStreamList.size(); ++i) {
            WeakReference<Thread> streamThread = new WeakReference<>(new Thread(new RunnableKafkaMessageCollector(topicName, kafkaStreamList.get(i))));

            streamThreadList.add(streamThread);
            streamThread.get().start();
        }

        while (true) {
            try {
                TimeUnit.MILLISECONDS.sleep(PARTITION_STATUS_CHECK_CYCLE);
                for (int i = 0; i < streamThreadList.size(); ++i) {
                    if (!streamThreadList.get(i).get().isAlive()) {
                        log.warn("Restarting " + topicName + " dispatcher - partition #" + i);
                        WeakReference<Thread> streamThread = new WeakReference<>(new Thread(new RunnableKafkaMessageCollector(topicName, kafkaStreamList.get(i))));

                        streamThreadList.set(i, streamThread);
                        streamThread.get().start();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
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

    @Data
    @AllArgsConstructor
    private class RunnableKafkaMessageCollector implements Runnable {
        private final String topicName;
        private final KafkaStream<byte[], byte[]> kafkaStream;

        @Override
        public void run() {
            final MessageDistributor messageDistributor = new MessageDistributorImpl(topicName);
            for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : kafkaStream) {
                final String message = new String(messageAndMetadata.message());
                log.info(topicName + " : " + message);

                if (!messageDistributor.sendMessageToHadoop(message))
                    log.error("Fail to send message to Hadoop");

                /* if you want to send message to database, delete comments below */
//                if (!messageDistributor.sendMessageToDatabase(message))
//                    log.error("Fail to send message to Database");
            }
        }
    }
}