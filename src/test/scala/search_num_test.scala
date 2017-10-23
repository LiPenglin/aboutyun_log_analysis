import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by peerslee on 17-4-15.
  */
object SearchNum_5_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ModulePV").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读取log
    val blogPat = ".*?\\shttp://www\\.aboutyun\\.com/search\\.php\\?mod=blog\\s.*?".r
    val forumPat = ".*?\\shttp://www\\.aboutyun\\.com/search\\.php\\?mod=forum\\s.*?".r

    // 各个模块访问次数
    val idRdd = sc.textFile("/home/peerslee/spark_data/ex17032606.log").map(lines => {
      var res : Any = None
      lines match {
        case blogPat() => res = "blog"
        case forumPat() => res = "forum"
        case _ => res = "no"
      }
      res
    }
    ).filter(!_.equals("no")).map((_, 1L)).reduceByKey(_+_)

    idRdd.foreach(println)
    sc.stop()
  }
}
