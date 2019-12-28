package com.xbgh.gmall.realtime.bean

/**
  * 事件日志
  * @param mid
  * @param uid
  * @param appid
  * @param area
  * @param os
  * @param ch
  * @param `type`
  * @param evid
  * @param pgid
  * @param npgid
  * @param itemid
  * @param logDate
  * @param logHour
  * @param ts
  */
case class EventInfo(mid:String,
                     uid:String,
                     appid:String,
                     area:String,
                     os:String,
                     ch:String,
                     `type`:String,
                     evid:String ,
                     pgid:String ,
                     npgid:String ,
                     itemid:String,
                     var logDate:String,
                     var logHour:String,
                     var ts:Long
                    )

