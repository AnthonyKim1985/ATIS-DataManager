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
public class MybiCard extends KafkaDataFormat {
    private Integer cardId;
    private String bzRegNumber;
    private String company;
    private String cardTransactionDate;
    private String cardNumber;
    private String cardTradeTime;
    private String cardCompanyType;
    private String cardCompanyName;
    private String terminalCode;
    private String busType;
    private String carNumber;
    private String routeCode;
    private String stationCode;
    private String stationName;
    private String adult;
    private String student;
    private String child;
    private String other;
    private String transactionAmount;
    private String transferFlag;
    private String prevRouteCode;
    private String prevTransactionDate;
    private String prevTransactionTime;
    private String prevStationCode;
    private String prevStationName;
    private String fileName;

    @Override
    public void buildObject(String json) {
        Gson gson = new GsonBuilder().create();
        MybiCard mybiCard = gson.fromJson(json, MybiCard.class);

        cardId = mybiCard.getCardId();
        bzRegNumber = mybiCard.getBzRegNumber();
        company = mybiCard.getCompany();
        cardTransactionDate = mybiCard.getCardTransactionDate();
        cardNumber = mybiCard.getCardNumber();
        cardTradeTime = mybiCard.getCardTradeTime();
        cardCompanyType = mybiCard.getCardCompanyType();
        cardCompanyName = mybiCard.getCardCompanyName();
        terminalCode = mybiCard.getTerminalCode();
        busType = mybiCard.getBusType();
        carNumber = mybiCard.getCarNumber();
        routeCode = mybiCard.getRouteCode();
        stationCode = mybiCard.getStationCode();
        stationName = mybiCard.getStationName();
        adult = mybiCard.getAdult();
        student = mybiCard.getStudent();
        child = mybiCard.getChild();
        other = mybiCard.getOther();
        transactionAmount = mybiCard.getTransactionAmount();
        transferFlag = mybiCard.getTransferFlag();
        prevRouteCode = mybiCard.getPrevRouteCode();
        prevTransactionDate = mybiCard.getPrevTransactionDate();
        prevTransactionTime = mybiCard.getPrevTransactionTime();
        prevStationCode = mybiCard.getPrevStationCode();
        prevStationName = mybiCard.getPrevStationName();
        fileName = mybiCard.getFileName();
    }
}
