package com.anthonykim.datamgr.job;

import lombok.extern.log4j.Log4j;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.List;

/**
 * Created by hyuk0 on 2016-11-03.
 */
@Log4j
public class TestJob implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();

        String topicName = jobDataMap.getString("topicName");
        List<Integer> messageList = (List<Integer>) jobDataMap.get("messageList");

        synchronized (messageList) {
            justRun(messageList, topicName);
        }
    }

    private static synchronized void justRun(List<Integer> messageList, String topicName) {
        try {
            log.info(topicName + " job is started.");
            log.info(messageList.size());
            messageList.clear();
            log.info( messageList.size());
            Thread.sleep(4000);
            log.info(topicName + " job is done.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
