package com.anthonykim.datamgr.dispatcher;

import com.anthonykim.datamgr.collector.MessageCollector;
import com.anthonykim.datamgr.common.PropertiesUtil;

import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;

import org.springframework.context.ApplicationContext;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by hyuk0 on 2016-10-28.
 */
@Log4j
@NoArgsConstructor
public class MessageCollectorDispatcherImpl implements MessageCollectorDispatcher {
    private ApplicationContext applicationContext;

    @Override
    public ApplicationContext getApplicationContext() {
        return this.applicationContext;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void run() {
        log.info("MessageCollectorDispatcher started...");
        List<WeakReference<Thread>> collectorThreadList = new ArrayList<>();

        Properties kafkaTopicProps = PropertiesUtil.getExternalProperties("conf/kafka-topic.properties");
        if (kafkaTopicProps == null)
            kafkaTopicProps = PropertiesUtil.getDefaultProperties("META-INF/properties/default-kafka-topic.properties");

        final String kafkaProperties[] = kafkaTopicProps.getProperty("kafka.topics").replaceAll("[ \t]", "").split("[,]");

        for (String kafkaProperty : kafkaProperties) {
            String[] topicAndPartitionAmount = kafkaProperty.split("[:]");

            WeakReference<Thread> collectorThread = getNewCollectorThread(topicAndPartitionAmount);
            collectorThreadList.add(collectorThread);
            collectorThread.get().start();
        }

        while (true) {
            try {
                TimeUnit.MILLISECONDS.sleep(MESSAGE_COLLECTOR_STATUS_CHECK_CYCLE);
                for (int i = 0; i < collectorThreadList.size(); ++i) {
                    if (!collectorThreadList.get(i).get().isAlive()) {
                        String[] topicAndPartitionAmount = kafkaProperties[i].split("[:]");
                        log.warn("Restarting " + topicAndPartitionAmount[PROPS_TOPIC_FIELD] + " dispatcher thread...");

                        WeakReference<Thread> collectorThread = getNewCollectorThread(topicAndPartitionAmount);
                        collectorThreadList.set(i, collectorThread);
                        collectorThread.get().start();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private WeakReference<Thread> getNewCollectorThread(String[] topicAndPartitionAmount) {
        MessageCollector messageCollector = applicationContext.getBean(MessageCollector.class);

        messageCollector.setTopicName(topicAndPartitionAmount[PROPS_TOPIC_FIELD]);
        messageCollector.setPartitionAmount(Integer.parseInt(topicAndPartitionAmount[PROPS_PARTITION_FIELD]));

        WeakReference<Thread> newCollectorThread = new WeakReference<>(new Thread(messageCollector));

        return newCollectorThread;
    }
}