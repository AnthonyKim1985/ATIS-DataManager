package com.anthonykim.datamgr.scheduler;

import com.anthonykim.datamgr.job.TestJob;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hyuk0 on 2016-11-03.
 */
@Log4j
public class QuartzSchedulerTest {
    private SchedulerFactory schedulerFactory;
    private Scheduler scheduler;

    @Before
    public void setup() throws SchedulerException {
        PropertyConfigurator.configure(QuartzSchedulerTest.class.getClassLoader().getResource("META-INF/config/log4j.properties"));
        schedulerFactory = new StdSchedulerFactory();
        scheduler = schedulerFactory.getScheduler();
        scheduler.start();
    }

    //@Test
    public void testQuartzScheduler() throws InterruptedException {
        Thread t1 = new Thread(new TestThread("T1"));
        Thread t2 = new Thread(new TestThread("T2"));
        Thread t3 = new Thread(new TestThread("T3"));
        Thread t4 = new Thread(new TestThread("T4"));
        Thread t5 = new Thread(new TestThread("T5"));

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();

        Thread.sleep(600000);
    }

    @After
    public void destroy() throws SchedulerException {
        scheduler.shutdown();
    }

    @Data
    @AllArgsConstructor
    private class TestThread implements Runnable {
        private String topicName;

        @Override
        public void run() {
            List<Integer> messageList = new ArrayList<>();

            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.put("topicName", topicName);
            jobDataMap.put("messageList", messageList);


            JobKey jobKey = new JobKey(topicName + "Job", "TestJob");
            JobDetail jobDetail = JobBuilder.newJob(TestJob.class).withIdentity(jobKey)
                    .setJobData(jobDataMap).storeDurably(true).build();

            TriggerKey triggerKey = new TriggerKey(topicName + "Trigger", "TestJobTrigger");
            Trigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/2 * * * * ?")).forJob(jobDetail).build();

            try {
                scheduler.scheduleJob(jobDetail, trigger);
            } catch (SchedulerException e) {
                e.printStackTrace();
            }

            int cnt = 0;
            while (true) {
                messageList.add(cnt++);
            }
        }
    }
}
