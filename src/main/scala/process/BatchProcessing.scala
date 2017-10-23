package processing

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import setting.Conf
import utils.MongoSQL

import scala.util.matching.Regex

/**
  * Created by peerslee on 17-4-26.
  */
object BatchProcessing {
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
  // 二、统计访问about云访问地区包括国内外及所占比率
  def IpLocation(sc : SparkContext, mongoSQL : MongoSQL) : Unit = {

    val ipRuelsRdd = sc.textFile("/home/peerslee/spark_data/ip.txt").map(line =>
      // map RDD 的Transformation 操作，用 f 处理一个Rdd 的所有元素，将结果输出到另一个Rdd
    {
      val fields = line.trim().split("\t")
      val start_num = ip2num(fields(0).trim())
      val end_num = ip2num(fields(1).trim())
      val province = fields(2).trim()
      (start_num, end_num, province)
    })
    // 将Rdd 转成Scala数组，并返回
    val ipRulesArray = ipRuelsRdd.collect()
    // 广播变量：保持一个缓存在每台机器上的只读变量
    val ipRulesBroadcast= sc.broadcast(ipRulesArray)
    // ip
    val ipPat = new Regex("((\\d{1,3}\\.){3}\\d{1,3})")
    // ip -> 访问次数 -> 本地数据
    val result = sc.textFile("/home/peerslee/spark_data/ex17032606.log")
      .map(line => { ipPat.findFirstIn(line.toString()).mkString("")})
      .map(ip => {
        var info : Any = None
        if(!ip.isEmpty) {
          val ipNum = ip2num(ip)
          val index = binarySearch(ipRulesBroadcast.value, ipNum)
          info = ipRulesBroadcast.value(index)
        }
        (info, 1L)})
      .reduceByKey(_+_)
    // total
    val total = result.reduce((x, y) => ("t", x._2+y._2))
    // 次数
    val rowRdd = result.map(x => {
      val r = x._2.toFloat/total._2
      (x._1, r)
    }).map(line => Row(line._1.toString, line._2.toString))
    val schemaStr = "loc,rate"
    mongoSQL.put(schemaStr, rowRdd, "IP_rate_2")
  }
  // 三、统计每天访问量比较高的前50篇文章
  def Top50(sc : SparkContext, mongoSQL: MongoSQL) : Unit = {
    val pat = "\\shttp://www\\.aboutyun\\.com/thread.*?\\.html.*?".r
    val rddArr = sc.textFile("/home/peerslee/spark_data/ex17032606.log")
      .map(lines => pat.findFirstIn(lines.toString()).mkString("").trim()).filter(!_.isEmpty)
      .map(lines => (lines, 1L)).reduceByKey(_ + _).map(e => (e._2, e._1)).sortByKey(false) take 50
    val rowRdd = sc.parallelize(rddArr).map(line => Row(line._1.toString, line._2.toString))
    val schemaStr = "num,url"
    mongoSQL.put(schemaStr, rowRdd, "Top50_3")
  }
  // 四、统计模块访问量并排序
  def ModulePV(sc : SparkContext, sqlContext : SQLContext, MongoSQL : MongoSQL) : Unit = {
    val df = sqlContext.read.json("/home/peerslee/spark_data/boutyun_plate_id.json").select(
      "id", "name", "rank"
    )

    val plateIdRuels = df.collect()
    val plateIdBroadcast = sc.broadcast(plateIdRuels)

    val idPat = new Regex("fid=\\d+")
    // 各个模块访问次数
    val rowRdd = sc.textFile("/home/peerslee/spark_data/ex17032606.log").map(lines =>
      idPat.findFirstIn(lines.toString()).mkString("").replace("fid=","")).filter(!_.isEmpty).map(id => {
      var res : Any = None
      plateIdBroadcast.value.foreach(bc => {
        if(bc(0).equals(id)) {
          res = bc
        }
      })
      (res, 1L)
    }).reduceByKey(_+_).map(e => (e._2, e._1)).sortByKey().filter(_._2 != None)
      .map(line => Row(line._1.toString, line._2.toString))
    val schemaStr = "num,type"
    MongoSQL.put(schemaStr, rowRdd, "module_pv_4")
  }
  // 五、统计使用搜索次数
  def SearchUseNum(sc : SparkContext, MongoSQL : MongoSQL) : Unit = {

    val blogPat = ".*?\\shttp://www\\.aboutyun\\.com/search\\.php\\?mod=blog\\s.*?".r
    val forumPat = ".*?\\shttp://www\\.aboutyun\\.com/search\\.php\\?mod=forum\\s.*?".r

    // 各个模块访问次数
    val rowRdd = sc.textFile("/home/peerslee/spark_data/ex17032606.log").map(lines => {
      var res : Any = None
      lines match {
        case blogPat() => res = "blog"
        case forumPat() => res = "forum"
        case _ => res = "no"
      }
      res
    }
    ).filter(!_.equals("no")).map((_, 1L)).reduceByKey(_+_)
      .map(line => Row(line._1.toString, line._2.toString))
    val schemaStr = "search,num"
    MongoSQL.put(schemaStr, rowRdd, "search_use_num_5")
  }
  // 六、统计导航的使用情况
  def Navi(sc : SparkContext, MongoSQL : MongoSQL) : Unit = {
    // 读取log
    val ddPat = ".*?\\shttp://www\\.aboutyun\\.com/forum\\.php\\?mod=guide\\s.*?".r
    val bkPat = ".*?\\shttp://www\\.aboutyun\\.com/home\\.php\\?mod=space&do=blog\\s.*?".r
    val ztPat = ".*?\\shttp://www\\.aboutyun\\.com/forum\\.php\\?mod=collection\\s.*?".r
    val llPat = ".*?\\shttp://www\\.aboutyun\\.com/home\\.php\\s.*?".r
    val ydPat = ".*?\\shttp://www\\.aboutyun\\.com/home\\.php\\?mod=space&do=share\\s.*?".r
    // 各个模块访问次数
    val rowRdd = sc.textFile("/home/peerslee/spark_data/ex17032606.log").map(lines => {
      var res : Any = None
      lines match {
        case ddPat() => res = "guide"
        case bkPat() => res = "blog"
        case ztPat() => res = "collection"
        case llPat() => res = "home"
        case ydPat() => res = "space"
        case _ => res = "no"
      }
      res
    }
    ).filter(!_.equals("no"))
      .map((_, 1L)).reduceByKey(_+_)
      .map(line => Row(line._1.toString, line._2.toString))
    val schemaStr = "type,num"
    MongoSQL.put(schemaStr, rowRdd, "navi_6")
  }

  def main(args: Array[String]): Unit = {

    val c = new Conf()
    val conf = new SparkConf().setAppName("BatchProcessing").setMaster("local[*]")
      .set("spark.mongodb.output.uri", c.MONGO_OUTPUT)

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val MongoSQL = new MongoSQL(sc)

//    println("Batching...1")
//    IpLocation(sc, MongoSQL)
//    println("Batching...2")
//    Top50(sc, MongoSQL)
//    println("Batching...3")
//    ModulePV(sc, sqlContext, MongoSQL)
    println("Batching...4")
    SearchUseNum(sc, MongoSQL)
    println("Batching...5")
    Navi(sc, MongoSQL)

    sc.stop()
  }

}
