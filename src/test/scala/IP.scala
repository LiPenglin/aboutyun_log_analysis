import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * Created by peerslee on 17-3-10.
  */
object IP {
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
  def main (args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocationByTime").setMaster("local")
    val sc = new SparkContext(conf)

    // 加载ip属地规则
    val ipRuelsRdd = sc.textFile("/home/peerslee/spark_data/ip_part1.txt").map(line =>
      // map RDD 的Transformation 操作，用 f 处理一个Rdd 的所有元素，将结果输出到另一个Rdd
    {
      val fields = line.trim().split("\t")
      val start_num = ip2num(fields(0).trim())
      val end_num = ip2num(fields(1).trim())
      val province = fields(2).trim()
      (start_num, end_num, province)
    })
    ipRuelsRdd.collect().foreach(println)
    sc.stop()

  }

}
