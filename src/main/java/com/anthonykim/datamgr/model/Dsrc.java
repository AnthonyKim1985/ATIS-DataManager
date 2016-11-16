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
public class Dsrc extends KafkaDataFormat {
    private Integer dsrcId;
    private String genTime;
    private String emType;
    private String emId;
    private String obuId;
    private String vehType;

    @Override
    public void buildObject(String json) {
        Gson gson = new GsonBuilder().create();
        Dsrc dsrc = gson.fromJson(json, Dsrc.class);

        dsrcId = dsrc.getDsrcId();
        genTime = dsrc.getGenTime();
        emType = dsrc.getEmType();
        emId = dsrc.getEmId();
        obuId = dsrc.getObuId();
        vehType = dsrc.getVehType();
    }
}