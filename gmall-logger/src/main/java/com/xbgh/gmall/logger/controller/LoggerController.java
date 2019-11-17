package com.xbgh.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xbgh.gmall.common.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController //Controller+ResponseBoye
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;


    @PostMapping("/log")
    public String log(@RequestParam("logString") String logString){

        //System.out.println(logString);
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());

        // 1 落盘 file
       /*String jsonString = jsonObject.toJSONString();
        log.info(jsonObject.toJSONString());*/

       //推送kafka
       if("startup".equals(jsonObject.get("type"))){
           kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());

       }else{
           kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());

       }

        return "success";


    }
}
