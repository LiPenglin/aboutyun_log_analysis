import kafka.api.OffsetRequest
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.matching.Regex

/**
  * Created by peerslee on 17-3-23.
  */
object PageView {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageView").setMaster("local[*]")
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

    // ip
    val ipPat = new Regex("((\\d{1,3}\\.){3}\\d{1,3})")
    // url
    val urlPat = new Regex("((http|https)://.*?)\\s")

    val msg = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2).map(line =>
    {
      (ipPat.findFirstIn(line), urlPat.findFirstIn(line))
    }).groupByKey()

    msg.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
