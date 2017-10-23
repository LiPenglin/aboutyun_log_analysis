package processing

import delivery.KafkaMsg
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import setting.Conf
import utils.MongoSQL

import scala.util.matching.Regex
/**
  * Created by peerslee on 17-4-25.
  */
object StreamProcessing {

  // ip
  val ipPat = new Regex("((\\d{1,3}\\.){3}\\d{1,3})")
  // url
  val urlPat = new Regex("((http|https)://.*?)\\s")

  // 将ip地址转换为整数
  def ip2num(ip : String) : Long = {
    val fragments = ip.split("\\.")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      // 与运算
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
  // 折半查找
  def binarySearch(lines:Array[(Long,Long,String)],ip:Long): Int ={
    var low =0
    var high = lines.length-1
    while (low<=high){
      val  middle = (low + high)/2
      if((ip>=lines(middle)._1)&&(ip<=lines(middle)._2)){
        return middle
      }
      if(ip<lines(middle)._1){
        high=middle-1
      }else{
        low = middle +1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {

    val c = new Conf()
    val conf = new SparkConf().setAppName("StreamProcessing").setMaster("local[*]")
      .set("spark.mongodb.output.uri", c.MONGO_OUTPUT)

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val mongoSQL = new MongoSQL(sc)

    // process
    val kafkaMsg = new KafkaMsg()
    val lines = kafkaMsg.Processing(ssc, "aboutyunlog", "consumer")

    PV(lines, mongoSQL)
    DirectOrInDirect(lines, mongoSQL)
    DangerIP(lines, sc, mongoSQL)

    ssc.start()
    ssc.awaitTermination()
  }
  // 一、统计访问量,pv,每个人访问该网站的数量,[uv,一个网站被多少人访问]
  def PV(dStream : DStream[String], mongoSQL: MongoSQL) : Unit = {

    dStream.map((line: String) => (ipPat.findFirstIn(line).mkString(""), 1L))
      .reduceByKey(_ + _).foreachRDD((rdd : RDD[(String, Long)]) => {
      val rowRdd = rdd.map(r => Row(r._1.toString, r._2.toString))
      val schemaStr = "ip,num"
      mongoSQL.put(schemaStr, rowRdd, "pv_1")
      println("1...ok")
    })
  }

  // 七、统计直接访问量及间接访问量
  def DirectOrInDirect(dStream : DStream[String], mongoSQL: MongoSQL) : Unit = {
    dStream.map(line => {
      val url = urlPat.findFirstIn(line)
      var key = "direct"
      if(!url.isEmpty) {
        key = "inDirect"
      }
      (key, 1L)
    }).reduceByKey(_+_).foreachRDD((rdd : RDD[(String, Long)]) => {
      val rowRdd = rdd.map(r => Row(r._1.toString, r._2.toString))
      val schemaStr = "type,num"
      mongoSQL.put(schemaStr, rowRdd, "direct_or_indirect_7")
      println("2...ok")
    })
  }
  // 八、当出现攻击时，统计出可疑10个ip
  def DangerIP(rdd : DStream[String],sc : SparkContext, mongoSQL: MongoSQL) : Unit = {
    rdd.filter(_ != "").map(line => {
      (ipPat.findFirstIn(line).toString, 1L)
    }).reduceByKey(_+_).map(i => (i._2, i._1.toString())).foreachRDD{(rdd : RDD[(Long, String)]) => {
        val sort = rdd.sortByKey(false) take 10 // array
        val schemaStr = "num,ip"
        val rowRdd = sc.parallelize(sort).map(r => Row(r._1.toString, r._2.toString))
        mongoSQL.put(schemaStr, rowRdd, "danger_IP_8")
        println("3...ok")
      }
    }
  }

}

