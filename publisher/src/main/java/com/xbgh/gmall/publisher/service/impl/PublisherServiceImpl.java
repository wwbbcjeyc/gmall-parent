package com.xbgh.gmall.publisher.service.impl;

import com.xbgh.gmall.publisher.mapper.DauMapper;
import com.xbgh.gmall.publisher.mapper.OrderMapper;
import com.xbgh.gmall.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
   DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDauTotal(String date) {
       return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHours(String date) {
        HashMap dauHourMap=new HashMap();
        //变换结构[{"LH":"11","CT""489},{"LH":"12","CT""123},{"LH":"13","CT""222},{"LH":"14","CT""323},{"LH":"15","CT""555}]
        //===》{"11":383,"12":123,"17":88,"19":200 }
        List<Map> dauHourList = dauMapper.selectDauTotalHourMap(date);
        for (Map map : dauHourList) {
            dauHourMap.put(map.get("LH"),map.get("CT"));
        }
        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHourMap(date);
      Map<String,Double> orderAmountHourMap = new HashMap<>();
        for (Map map : mapList) {
            orderAmountHourMap.put((String)map.get("CREATE_HOUR"),(Double)map.get("SUM_AMOUNT"));
        }
        return orderAmountHourMap;
    }
}
