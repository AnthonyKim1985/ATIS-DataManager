package com.anthonykim.datamgr.distributor;

/**
 * Created by hyuk0 on 2016-11-04.
 */
public interface MessageDistributor {
    boolean sendMessageToHadoop(final String message);

    boolean sendMessageToDatabase(final String message);
}