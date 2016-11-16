package com.anthonykim.datamgr.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Created by hyuk0 on 2016-11-01.
 */
public abstract class KafkaDataFormat {
    public abstract void buildObject(String json);

    public String buildJson() {
        Gson gson = new GsonBuilder().create();
        String json = gson.toJson(this);

        return json;
    }
}