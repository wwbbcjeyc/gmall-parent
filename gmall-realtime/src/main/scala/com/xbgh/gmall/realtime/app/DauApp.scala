package com.xbgh.gmall.realtime.app

import com.xbgh.gmall.common.constants.GmallConstants
import com.xbgh.gmall.realtime.utils.MykafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {

  def main(args: Array[String]): Unit = {
  val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    inputDstream.foreachRDD{rdd=>
      //println(rdd.map(record=>record.value()).collect().mkString("\n"))
      println(rdd.map(_.value()).collect().mkString("\n"))
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
