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
public class Bis extends KafkaDataFormat {
    private Integer bisId;
    private String collectDate;
    private String busId;
    private String locationId;
    private String locationType;
    private String busRouteId;
    private String vehicleFacilityId;
    private String eventCode;
    private String serverTime;
    private String satelliteCount;
    private String systemDate;
    private String localCoordinatesX;
    private String localCoordinatesY;
    private String mapCoordinatesX;
    private String mapCoordinatesY;
    private String errorCode;
    private String eventSeq;
    private String commSeq;
    private String eventType;
    private String partitionField;
    private String modemSenseValue;
    private String doorStatus;
    private String preLocationId;
    private String routeDistance;

    @Override
    public void buildObject(String json) {
        Gson gson = new GsonBuilder().create();
        Bis bis = gson.fromJson(json, Bis.class);

        bisId = bis.getBisId();
        collectDate = bis.getCollectDate();
        busId = bis.getBusId();
        locationId = bis.getLocationId();
        locationType = bis.getLocationType();
        busRouteId = bis.getBusRouteId();
        vehicleFacilityId = bis.getVehicleFacilityId();
        eventCode = bis.getEventCode();
        serverTime = bis.getServerTime();
        satelliteCount = bis.getSatelliteCount();
        systemDate = bis.getSystemDate();
        localCoordinatesX = bis.getLocalCoordinatesX();
        localCoordinatesY = bis.getLocalCoordinatesY();
        mapCoordinatesX = bis.getMapCoordinatesX();
        mapCoordinatesY = bis.getMapCoordinatesY();
        errorCode = bis.getErrorCode();
        eventSeq = bis.getEventSeq();
        commSeq = bis.getCommSeq();
        eventType = bis.getEventType();
        partitionField = bis.getPartitionField();
        modemSenseValue = bis.getModemSenseValue();
        doorStatus = bis.getDoorStatus();
        preLocationId = bis.getPreLocationId();
        routeDistance = bis.getRouteDistance();
    }
}
