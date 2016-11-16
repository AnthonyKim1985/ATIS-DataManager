package com.anthonykim.datamgr.dispatcher;

import com.anthonykim.datamgr.common.PropertiesUtil;
import com.anthonykim.datamgr.spark.SparkStreaming;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.springframework.context.ApplicationContext;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by hyuk0 on 2016-11-08.
 */
@Log4j
@NoArgsConstructor
@Deprecated
public class SparkStreamingDispatcherImpl implements SparkStreamingDispatcher {
    private ApplicationContext applicationContext;

    @Override
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void run() {
        log.info("SparkStreamingDispatcher started...");

        List<WeakReference<Thread>> sparkStreamingThreadList = new ArrayList<>();
        Properties kafkaTopicProps = PropertiesUtil.getExternalProperties("conf/kafka-topic.properties");
        if (kafkaTopicProps == null)
            kafkaTopicProps = PropertiesUtil.getDefaultProperties("META-INF/properties/default-kafka-topic.properties");

        final String kafkaProperties[] = kafkaTopicProps.getProperty("kafka.topics").replaceAll("[ \t]", "").split("[,]");

        for (String kafkaProperty : kafkaProperties) {
            String[] topicAndPartitionAmount = kafkaProperty.split("[:]");

            WeakReference<Thread> sparkStreamingThread = getNewSparkStreamingThread(topicAndPartitionAmount);
            sparkStreamingThreadList.add(sparkStreamingThread);
            sparkStreamingThread.get().start();
        }
    }

    private WeakReference<Thread> getNewSparkStreamingThread(String[] topicAndPartitionAmount) {
        SparkStreaming sparkStreaming = applicationContext.getBean(SparkStreaming.class);

        sparkStreaming.setTopicName(topicAndPartitionAmount[PROPS_TOPIC_FIELD]);
        sparkStreaming.setPartitionAmount(Integer.parseInt(topicAndPartitionAmount[PROPS_PARTITION_FIELD]));

        WeakReference<Thread> newSparkStreamingThread = new WeakReference<>(new Thread(sparkStreaming));

        return newSparkStreamingThread;
    }
}