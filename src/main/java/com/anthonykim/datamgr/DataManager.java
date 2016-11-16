package com.anthonykim.datamgr;

import com.anthonykim.datamgr.dispatcher.MessageCollectorDispatcher;
import com.anthonykim.datamgr.dispatcher.SparkStreamingDispatcher;
import lombok.extern.log4j.Log4j;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.lang.ref.WeakReference;
import java.util.concurrent.TimeUnit;

/**
 * Created by hyuk0 on 2016-10-27.
 */
@Log4j
public class DataManager {
    private static Long DAEMON_CHECK_CYCLE = 60000L;
    private static ApplicationContext applicationContext;

    static {
        PropertyConfigurator.configure(DataManager.class.getClassLoader().getResource("META-INF/config/log4j.properties"));
        applicationContext = new ClassPathXmlApplicationContext("META-INF/spring/datamgr-context.xml");
    }

    public static void main(String[] args) {
        log.info("DataManager stated...");

        WeakReference<Thread> messageCollectorDispatcherThread = getMessageCollectorDispatcherThread();
//        WeakReference<Thread> sparkStreamingDispatcherThread = getSparkStreamingDispatcherThread();

        messageCollectorDispatcherThread.get().start();
        log.info("MessageCollectorDispatcher started...");

//        sparkStreamingDispatcherThread.get().start();
//        log.info("SparkStreamingDispatcher started...");

        while (true) {
            try {
                TimeUnit.MILLISECONDS.sleep(DAEMON_CHECK_CYCLE);
                if (!messageCollectorDispatcherThread.get().isAlive()) {
                    log.warn("Restarting MessageCollectorDispatcher ...");

                    messageCollectorDispatcherThread = getMessageCollectorDispatcherThread();
                    messageCollectorDispatcherThread.get().start();

                    log.info("MessageCollectorDispatcher started...");
                }

//                if (!sparkStreamingDispatcherThread.get().isAlive()) {
//                    log.warn("Restarting SparkStreamingDispatcher ...");
//
//                    sparkStreamingDispatcherThread = getSparkStreamingDispatcherThread();
//                    sparkStreamingDispatcherThread.get().start();
//
//                    log.info("SparkStreamingDispatcher started...");
//                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static WeakReference<Thread> getMessageCollectorDispatcherThread() {
        MessageCollectorDispatcher messageCollectorDispatcher = applicationContext.getBean(MessageCollectorDispatcher.class);
        messageCollectorDispatcher.setApplicationContext(applicationContext);

        WeakReference<Thread> messageCollectorDispatcherThread = new WeakReference<>(new Thread(messageCollectorDispatcher));
        messageCollectorDispatcherThread.get().setDaemon(true);

        return messageCollectorDispatcherThread;
    }

//    private static WeakReference<Thread> getSparkStreamingDispatcherThread() {
//        SparkStreamingDispatcher sparkStreamingDispatcher = applicationContext.getBean(SparkStreamingDispatcher.class);
//        sparkStreamingDispatcher.setApplicationContext(applicationContext);
//
//        WeakReference<Thread> sparkStreamingDispatcherThread = new WeakReference<>(new Thread(sparkStreamingDispatcher));
//        sparkStreamingDispatcherThread.get().setDaemon(true);
//
//        return sparkStreamingDispatcherThread;
//    }
}