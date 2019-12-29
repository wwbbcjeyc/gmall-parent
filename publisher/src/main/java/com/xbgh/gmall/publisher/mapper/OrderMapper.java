package com.xbgh.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    //1 查询当日交易额总数

    public Double selectOrderAmount(String date);

    //2 查询当日交易额分时明细
    public List<Map> selectOrderAmountHour(String date);







}
