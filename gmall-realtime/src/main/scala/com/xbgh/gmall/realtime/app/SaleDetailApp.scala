package com.xbgh.gmall.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.xbgh.gmall.common.constants.GmallConstants
import com.xbgh.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.xbgh.gmall.realtime.utils.{MyEsUtil, MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("sale_detail_app").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val orderInputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_ORDER,ssc)
    val orderDetailInputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_ORDER_DETAIL,ssc)

    // 变换结构  record =>  case Class
    val orderDstream: DStream[OrderInfo] = orderInputDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val createtimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createtimeArr(0)
      orderInfo.create_hour = createtimeArr(1).split(":")(0)
      val tel3_8: (String, String) = orderInfo.consignee_tel.splitAt(3)
      val front3: String = tel3_8._1 //138****1234
    val back4: String = tel3_8._2.splitAt(4)._2
      orderInfo.consignee_tel = front3 + "****" + back4
      orderInfo
    }

    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputDstream.map { record =>
      val jsonString: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }


    val orderInfoWithKeyDstream: DStream[(String, OrderInfo)] = orderDstream.map{orderInfo=>(orderInfo.id,orderInfo)}
    val orderDetailWithKeyDstream: DStream[(String, OrderDetail)] = orderDetailDstream.map{orderDetail=>(orderDetail.order_id,orderDetail)}
    //双流join
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)

    val saleDetailDstream: DStream[SaleDetail] = fullJoinDStream.mapPartitions { fulljoinResultItr =>
      //当前partition 中最终join成功的结果
      val saleDetailList: ListBuffer[SaleDetail] = ListBuffer[SaleDetail]()
      implicit val formats = org.json4s.DefaultFormats
      val jedis: Jedis = RedisUtil.getJedisClient
      for ((orderId, (orderInfoOpt, orderDetailOpt)) <- fulljoinResultItr) {

        if (orderInfoOpt != None) {
          val orderInfo: OrderInfo = orderInfoOpt.get
          // 1 判断 detail 如果存在  生成一个 saleDetail
          if (orderDetailOpt != None) {
            val orderDetail: OrderDetail = orderDetailOpt.get
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }
          // 2  把自己的数据写入到缓存中
          // redis     1 type  string    2 key   order_info:[orderId]  3 value  order_info json

          val orderInfoKey = "order_info:" + orderInfo.id
          //fast转case class 但把case class 转json 要使用json4s
          val orderInfoJson: String = Serialization.write(orderInfo)
          //  val orderInfoJson: String = JSON.toJSONString()
          jedis.setex(orderInfoKey, 600, orderInfoJson)


          // 3  查询缓存中的Detail 是否能跟自己匹配 如果匹配  生成一个saleDetail
          val orderDetailKey = "orderDetail:" + orderInfo.id
          val orderDetailSet: util.Set[String] = jedis.smembers(orderDetailKey)
          util.Set
          if (orderDetailSet != null && orderDetailSet.size() > 0) {
            import scala.collection.JavaConversions._
            for (orderDetailJson <- orderDetailSet) {
              val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              saleDetailList += saleDetail
            }

          }
        } else {
          val orderDetail: OrderDetail = orderDetailOpt.get
          //1 写缓存
          // redis  type  :  set         key:   order_detail:[orderId]     value :  orderDetailJson 多个

          val orderDetailKey = "orderDetail:" + orderDetail.order_id
          val orderDetailJson: String = Serialization.write(orderDetail)
          jedis.sadd(orderDetailKey, orderDetailJson)
          jedis.expire(orderDetailKey, 600);

          //2 从缓存中读取 ，看是否有匹配
          val orderInfoKey = "order_info:" + orderDetail.order_id
          val orderInfoJson: String = jedis.get(orderInfoKey)
          if (orderInfoJson != null) { //缓存里如果存在  进行关联处理 生成saleDetail 放入结果列表
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            saleDetailList += saleDetail
          }

        }
      }
      jedis.close()
      saleDetailList.toIterator
    }

    //写入到ES 中
    saleDetailDstream.foreachRDD{rdd=>
      rdd.foreachPartition{saleDetailItr=>
        val saleDetailWithIdItr: Iterator[(String, SaleDetail)] = saleDetailItr.map(saleDetail=>(saleDetail.order_detail_id,saleDetail))
        MyEsUtil.insertBulk(saleDetailWithIdItr.toList,GmallConstants.ES_INDEX_ALERT,GmallConstants.ES_DEFAULT_TYPE)
      }

    }




    /*fullJoinDStream.foreachRDD{rdd=>
      rdd.foreach{case(orderId,(orderInfo,orderDetail))=>
      println(orderId+"||"+orderInfo+"||"+orderDetail)
      }
    }*/


    ////////////////////////////////  同步user_info 到redis 库

    val userInputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_USER,ssc)
    //redis      type   string    key    user_info:[user_id]   value userInfoJson
    val userInfoDstream: DStream[UserInfo] = userInputDstream.map { record =>
      val userInfoJson: String = record.value()
      val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
      userInfo
    }
    userInfoDstream.foreachRDD{ rdd=>
      rdd.foreachPartition{userInfoItr=>
        val jedis: Jedis = RedisUtil.getJedisClient
        implicit val formats = org.json4s.DefaultFormats
        for ( userInfo<- userInfoItr ) {
          val userInfoJson: String = Serialization.write(userInfo)
          val userInfoKey="user_info:"+userInfo.id
          jedis.set(userInfoKey,userInfoJson)
        }
        jedis.close()
      }


    }

    ssc.start()
    ssc.awaitTermination()

  }
}
