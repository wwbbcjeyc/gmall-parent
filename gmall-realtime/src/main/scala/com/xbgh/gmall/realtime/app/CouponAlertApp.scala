package com.xbgh.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.xbgh.gmall.common.constants.GmallConstants
import com.xbgh.gmall.realtime.bean.{CouponAlertInfo, EventInfo}
import com.xbgh.gmall.realtime.utils.{MyEsUtil, MykafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._



object CouponAlertApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("couponAlertApp")
        val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)



    //调整结构
    val eventDstream: DStream[EventInfo] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonString, classOf[EventInfo])
      val formator = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateHour: String = formator.format(new Date(eventInfo.ts))
      val dateHourArr: Array[String] = dateHour.split(" ")
      eventInfo.logDate = dateHourArr(0)
      eventInfo.logHour = dateHourArr(1)
      eventInfo
    }

    //开窗
    val eventWindowDstream: DStream[EventInfo] = eventDstream.window(Seconds(300),Seconds(10))
    //分组
    val eventGroipbyMidDstream: DStream[(String, Iterable[EventInfo])] = eventDstream.map(eventInfo=>(eventInfo.mid,eventInfo)).groupByKey()

    //筛选
    //三次几以上领取优惠券 //在组内根据mid的时间集合进行 过滤筛选
    //用不同账号登录
    //在过程中没有浏览商品
    //1.判断出来是否达到预警的要求
    //2.如果达到要求组织预警的信息
    val alterDstream: DStream[(Boolean, CouponAlertInfo)] = eventGroipbyMidDstream.map { case (mid, eventInfoItr) =>
      var ifAlert = true
      //登录过的uid
      val uidSet = new util.HashSet[String]()
      //领取的商品id
      val itemIdSet = new util.HashSet[String]()
      //做过哪些行为
      val eventList = new util.ArrayList[String]()

      breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          if (eventInfo.evid == "coupon") {
            //购物券
            uidSet.add(eventInfo.uid)
            itemIdSet.add(eventInfo.itemid)
          }
          eventList.add(eventInfo.evid)
          if (eventInfo.evid == "clickItem") {
            //浏览商品
            ifAlert = false
            break()
          }
        }
      )
      if (uidSet.size() < 3) {
        //超过3个及以上账号登录 符合预警
        ifAlert = false
      }
      (ifAlert, CouponAlertInfo(mid, uidSet, itemIdSet, eventList, System.currentTimeMillis()))
    }

    //过滤
    //过滤
    val filterAlertDstream: DStream[(Boolean, CouponAlertInfo)] = alterDstream.filter(_._1 )
    //  转换结构(ifAlert,alerInfo) =>（ mid_minu  ,  alerInfo）
    val alertInfoWithIdDstream: DStream[(String, CouponAlertInfo)] = filterAlertDstream.map { case (ifAlert, alertInfo) =>
      val uniKey = alertInfo.mid + "_" + alertInfo.ts / 1000 / 60
      (uniKey, alertInfo)
    }

    //存储-es 提前建立index 和mapping
    /*alterDstream.foreachRDD{rdd=>
     println( rdd.collect().mkString("\n") )
    }*/
    //5 存储->es   提前建好index  和 mapping
    alertInfoWithIdDstream.foreachRDD{rdd=>
      rdd.foreachPartition{alertInfoItr=>
        val alertList: List[(String, CouponAlertInfo)] = alertInfoItr.toList
        MyEsUtil.insertBulk(alertList ,GmallConstants.ES_INDEX_ALERT,GmallConstants.ES_DEFAULT_TYPE )
      }

    }




    ssc.start()
    ssc.awaitTermination()

  }
}
