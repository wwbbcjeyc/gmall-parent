package com.xbgh.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xbgh.gmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String dateString){
        // 日活总数
        Long dauTotal = publisherService.getDauTotal(dateString);

        List<Map> totalList =new ArrayList<>();
        HashMap dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        totalList.add(dauMap);


        HashMap midMap = new HashMap();
        midMap.put("id","mid");
        midMap.put("name","新增设备");
        midMap.put("value",323);

        totalList.add(midMap);

        //总交易额
        Double orderAmount = publisherService.getOrderAmount(dateString);
        HashMap orderAmountMap = new HashMap<>();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value",orderAmount);

        totalList.add(orderAmountMap);

       return JSON.toJSONString(totalList);

    }

    @GetMapping("realtime-hours")
    public String realtimeHourDate(@RequestParam("id") String id,@RequestParam("date") String date){

        if( "dau".equals(id)){
            Map dauHoursToday = publisherService.getDauHours(date);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("today",dauHoursToday);
            String yesterday = getYesterday(date);
            Map dauHoursYesterday = publisherService.getDauHours(yesterday);
            jsonObject.put("yesterday",dauHoursYesterday);
            return jsonObject.toJSONString();
        }else if("order_amount".equals(id)) {
            Map orderAmountHourTD = publisherService.getOrderAmountHour(date);
            String yesterday = getYesterday(date);
            Map orderAmountHourYD = publisherService.getOrderAmountHour(yesterday);
            Map hourMap = new HashMap();
            hourMap.put("today",orderAmountHourTD);
            hourMap.put("yesterday",orderAmountHourYD);
            return JSON.toJSONString(hourMap);
        }
        return null;
    }

    private String getYesterday(String today){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date todayD = simpleDateFormat.parse(today);
            Date yesterdayD = DateUtils.addDays(todayD, -1);
            String yesterday = simpleDateFormat.format(yesterdayD);
            return yesterday;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }




}
