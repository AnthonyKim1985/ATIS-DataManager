package com.anthonykim.datamgr.scheduler;

import lombok.Data;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * Created by hyuk0 on 2016-11-02.
 */
public class JobScheduler {
    private static SchedulerFactory schedulerFactory;
    private static Scheduler scheduler;

    static {
        try {
            schedulerFactory = new StdSchedulerFactory();
            scheduler = schedulerFactory.getScheduler();
            scheduler.getListenerManager().addSchedulerListener(new JobSchedulerListener(scheduler));
            scheduler.start();
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    public static void addJob(JobDetail job, Trigger trigger) {
        try {
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }
}
