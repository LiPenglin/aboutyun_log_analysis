import kafka.api.OffsetRequest
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.matching.Regex

/**
  * Created by peerslee on 17-3-26.
  */
object DangerIp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DangerIp").setMaster("local[*]")
    // 10秒批间隔时间
    val ssc = new StreamingContext(conf, Seconds(10))
    // kafka集群中的一台或多台服务器统称为broker
    val brokers = "localhost:9092,localhost:9093,localhost:9094"

    // kafka 处理的消息源
    val topics = "aboutyunlog"
    val groupID = "consumer"
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupID,
      // 说明每次程序启动，从kafka中最开始的第一条消息开始读取
      "auto.offset.reset" -> OffsetRequest.SmallestTimeString
    )

    // url
    val ipPat = new Regex("((\\d{1,3}\\.){3}\\d{1,3})")
    // msg 是个DStream
    val msg = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      /*
      返回值它包含的数据类型是二元组(String, String)，
      因此，可以调用(_._2)得到二元组第二个元素
       */
      ssc, kafkaParams, topicsSet).map(_._2).filter(_ != "").map(line => {
      (ipPat.findFirstIn(line), 1L)
    }).reduceByKey(_+_).map(i => (i._2, i._1)).foreachRDD{
      rdd => {
        val sort = rdd.sortByKey(false) take 3
        for (s <- sort) {
          println(s)
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
