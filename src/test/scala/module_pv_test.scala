import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * Created by peerslee on 17-4-14.
  */
object ModulePV_4_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ModulePV").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // sqlContext
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("/home/peerslee/spark_data/boutyun_plate_id.json").select(
      "id", "name", "rank"
    )
//    df.show()
    // 获取所有数据到数组
    val plateIdRuels = df.collect()
//    plateIdRuels.map(p => println(p))
    // 广播
    val plateIdBroadcast = sc.broadcast(plateIdRuels)
    // 读取log
    // ip
    val idPat = new Regex("fid=\\d+")
    // 各个模块访问次数
    val idRdd = sc.textFile("/home/peerslee/spark_data/ex17032606.log").map(lines =>
      idPat.findFirstIn(lines.toString()).mkString("").replace("fid=","")).filter(!_.isEmpty).map(id => {
      var res : Any = None
      plateIdBroadcast.value.foreach(bc => {
        if(bc(0).equals(id)) {
          res = bc
        }
      })
      (res, 1L)
    }).reduceByKey(_+_).map(e => (e._2, e._1)).sortByKey().filter(_._2 != None)

    for (i <- idRdd.collect()) {
      println(i)
    }
    sc.stop()
  }

}
