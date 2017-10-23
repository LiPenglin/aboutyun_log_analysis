import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingDemo {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf对象，并指定AppName和Master
    val conf = new SparkConf().setAppName("StreamingDemo").setMaster("local[*]")
    // 创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(10))

    val hostname = "127.0.0.1" // 即我们的master虚拟机
    val port = 9999 // 端口号

    // 创建DStream对象
    val lines = ssc.socketTextStream(hostname, port, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}