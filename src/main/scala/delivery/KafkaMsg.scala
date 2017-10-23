package delivery

import kafka.api.OffsetRequest
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by peerslee on 17-4-25.
  */
class KafkaMsg {
  def Processing(ssc: StreamingContext, topics : String, groupID : String): DStream[String] ={
    // kafka集群中的一台或多台服务器统称为broker
    val brokers = "localhost:9092,localhost:9093,localhost:9094"
    // kafka
    val topicsSet = topics.split(",").toSet
    // 参数
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupID,
      // 说明每次程序启动，从kafka中最开始的第一条消息开始读取
      "auto.offset.reset" -> OffsetRequest.SmallestTimeString
    )
    // 返回一个DStream
    KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      .map(_._2)
  }

}
