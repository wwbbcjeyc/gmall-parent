package com.xbgh.gmall.realtime.bean

/**
  * 预警信息
  * @param mid
  * @param uids
  * @param itemIds
  * @param events
  * @param ts
  */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.List[String],
                           ts:Long)  {

}

