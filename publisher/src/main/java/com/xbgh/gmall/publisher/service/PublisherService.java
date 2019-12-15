package com.xbgh.gmall.publisher.service;

import java.util.Map;

public interface PublisherService {
    public Long getDauTotal(String date );

    public Map getDauHours(String date );

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);


}
