package com.anthonykim.datamgr.dispatcher;

import org.springframework.context.ApplicationContext;

/**
 * Created by hyuk0 on 2016-11-08.
 */
public interface Dispatcher extends Runnable {
    Integer PROPS_TOPIC_FIELD = 0;

    Integer PROPS_PARTITION_FIELD = 1;

    ApplicationContext getApplicationContext();

    void setApplicationContext(ApplicationContext applicationContext);
}
