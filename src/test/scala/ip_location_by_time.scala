import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * Created by peerslee on 17-3-10.
  */
object IpLocationByTime {
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
  def main (args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocationByTime").setMaster("local")
    val sc = new SparkContext(conf)

    // 加载ip属地规则
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

    // 处理加载的数据
    val ipsRDD = sc.textFile("/home/peerslee/spark_data/ex17020509.log").map(line =>
    {
      // 需要字符串
      ipPat.findFirstIn(line.toString()).mkString("")
    })
    // ((2007496192,2007496447,山东省青岛市,北京百度网讯科技有限公司联通节点),30)
    val result = ipsRDD.map(ip => {
      var info : Any = None
      if(!ip.isEmpty) {
        val ipNum = ip2num(ip)
        val index = binarySearch(ipRulesBroadcast.value, ipNum)
        info = ipRulesBroadcast.value(index)
      }
      (info, 1L)
    }).reduceByKey(_+_) // 按照key 求和
//    for(r <- result.collect()) {
//      println(r)
//    }

    // total:(Total,94)
    val total = result.reduce((x, y) => {
      val v = x._2 + y._2
      ("Total", v)
    })
    // ((2007496192,2007496447,山东省青岛市,北京百度网讯科技有限公司联通节点),0.31914893)
    val rate = result.map(x => {
      val r = x._2.toFloat / total._2
      (x._1, r)
    })
    for(r <- rate.collect()) {
      println(r)
    }

    sc.stop()

  }

}
