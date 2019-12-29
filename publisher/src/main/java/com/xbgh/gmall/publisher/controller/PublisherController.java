package com.xbgh.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xbgh.gmall.publisher.bean.Option;
import com.xbgh.gmall.publisher.bean.Stat;
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
        Long dauTotal = publisherService.getDauTotal(dateString);

        List<Map>  totalList=new ArrayList<>();
        HashMap dauMap = new HashMap();

        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        totalList.add(dauMap);


        HashMap midMap = new HashMap();

        midMap.put("id","new_mid");
        midMap.put("name","新增设备");
        midMap.put("value",323);

        totalList.add(midMap);

        // 总交易额
        Double orderAmount = publisherService.getOrderAmount(dateString);
        HashMap orderAmountMap = new HashMap();

        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value",orderAmount);

        totalList.add(orderAmountMap);


        return  JSON.toJSONString(totalList);

    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id ,@RequestParam("date") String dateString){
        if("dau".equals(id)) {
            Map<String, Long> dauTotalHoursTD = publisherService.getDauTotalHours(dateString);
            String yesterday = getYesterday(dateString);
            Map<String, Long> dauTotalHoursYD = publisherService.getDauTotalHours(yesterday);

            Map hourMap = new HashMap();
            hourMap.put("today", dauTotalHoursTD);
            hourMap.put("yesterday", dauTotalHoursYD);

            return JSON.toJSONString(hourMap);
        }else if("order_amount".equals(id)){
            Map<String, Double> orderAmountHoursTD = publisherService.getOrderAmountHours(dateString);
            String yesterday = getYesterday(dateString);
            Map<String, Double> orderAmountHoursYD = publisherService.getOrderAmountHours(yesterday);

            Map hourMap = new HashMap();
            hourMap.put("today", orderAmountHoursTD);
            hourMap.put("yesterday", orderAmountHoursYD);

            return JSON.toJSONString(hourMap);
        }

        return  null;
    }

    private String   getYesterday(String today){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        try {
            Date todayD = simpleDateFormat.parse(today);
            Date yesterdayD = DateUtils.addDays(todayD, -1);
            String yesterday = simpleDateFormat.format(yesterdayD);
            return  yesterday;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;

    }

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage")int startPage,@RequestParam("size")int size,@RequestParam("keyword")String keyword){

        Map saleDetailMap = publisherService.getSaleDetailMap(date, keyword, startPage, size);

        Map genderMap = (Map)saleDetailMap.get("genderMap"); //F:102,M:232
        Map ageMap = (Map)saleDetailMap.get("ageMap");
        Long total = (Long)saleDetailMap.get("total");

        //性别饼图数据
        Long femaleCount = (Long) genderMap.get("F");
        Long maleCount = (Long) genderMap.get("M");

        Double femaleRatio = Math.round(femaleCount*1000D/total) / 10D;
        Double maleRatio = Math.round(maleCount*1000D/total) / 10D;

        List<Option> genderOptions = new ArrayList<>();

        genderOptions.add( new Option("男", maleRatio));
        genderOptions.add( new Option("女", femaleRatio));

        Stat genderStat = new Stat("性别占比", genderOptions);

        //年龄段饼图数据
        //把年龄个数转换成年龄占比

        Long age_20=0L;
        Long age_20_30=0L;
        Long age_30_=0L;


        for (Object o : ageMap.entrySet()) {
             Map.Entry entry = (Map.Entry)o;
            String age =(String) entry.getKey();
            Long count =(Long) entry.getValue();
            Integer ageIt = Integer.valueOf(age);

            if(ageIt < 20){
                age_20+=count;
            }else if(ageIt >= 20 && ageIt < 30){
                age_20_30+=count;
            }else{
                age_30_+=count;
            }
        }

        Double age_20Ratio=Math.round(age_20*1000D/total)/10D;
        Double age_20_30Ratio=Math.round(age_20_30*1000D/total)/10D;
        Double age_30_Ratio=Math.round(age_30_*1000D/total)/10D;

        ArrayList<Option> ageOptions = new ArrayList<>();
        ageOptions.add(new Option("20岁以下",age_20Ratio));
        ageOptions.add(new Option("20到30岁",age_20_30Ratio));
        ageOptions.add(new Option("30岁以上",age_30_Ratio));

        Stat ageStat = new Stat("用户年龄占比", ageOptions);

        List<Stat> statList=new ArrayList<>();
        statList.add(genderStat);
        statList.add(ageStat);

       Map resultMap = new HashMap();
        resultMap.put("total",total);
        resultMap.put("stat",statList);
        resultMap.put("detail",saleDetailMap.get("saleDetailList"));

        return JSON.toJSONString(resultMap);



    }

}