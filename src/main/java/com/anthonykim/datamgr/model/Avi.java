package com.anthonykim.datamgr.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Created by hyuk0 on 2016-11-01.
 */
@lombok.Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Avi extends KafkaDataFormat {
    private Integer aviId;
    private String genTime;
    private String emType;
    private String emId;
    private String vehNo;
    private String direction;
    private String roadNo;

    @Override
    public void buildObject(String json) {
        Gson gson = new GsonBuilder().create();
        Avi avi = gson.fromJson(json, Avi.class);

        aviId = avi.getAviId();
        genTime = avi.getGenTime();
        emType = avi.getEmType();
        emId = avi.getEmId();
        vehNo = avi.getVehNo();
        direction = avi.getDirection();
        roadNo = avi.getRoadNo();
    }
}