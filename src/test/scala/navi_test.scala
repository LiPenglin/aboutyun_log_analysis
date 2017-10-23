import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by peerslee on 17-4-15.
  */
object Navi_6_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ModulePV").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读取log
    val ddPat = ".*?\\shttp://www\\.aboutyun\\.com/forum\\.php\\?mod=guide\\s.*?".r
    val bkPat = ".*?\\shttp://www\\.aboutyun\\.com/home\\.php\\?mod=space&do=blog\\s.*?".r
    val ztPat = ".*?\\shttp://www\\.aboutyun\\.com/forum\\.php\\?mod=collection\\s.*?".r
    val llPat = ".*?\\shttp://www\\.aboutyun\\.com/home\\.php\\s.*?".r
    val ydPat = ".*?\\shttp://www\\.aboutyun\\.com/home\\.php\\?mod=space&do=share\\s.*?".r
    // 各个模块访问次数
    val idRdd = sc.textFile("/home/peerslee/spark_data/ex17032606.log").map(lines => {
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
    ).filter(!_.equals("no")).map((_, 1L)).reduceByKey(_+_)

    idRdd.foreach(println)
    sc.stop()
  }
}
