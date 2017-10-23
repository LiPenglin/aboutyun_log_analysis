/**
  * Created by peerslee on 17-4-15.
  */
object caseTest {
  def main(args: Array[String]): Unit = {
    val ddPat = ".*?mod=forum.*?".r
    val bkPat = ".*?http://www\\.aboutyun\\.com/home\\.php\\?mod=space&do=blog.*?".r
    val ztPat = ".*?http://www\\.aboutyun\\.com/home\\.php\\?mod=space&do=share.*?".r
    val input = "2017-03-26 06:50:13 GET /forum.php 222.216.190.161 Mozilla/5.0+(Windows+NT+6.1;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/45.0.2454.101+Safari/537.36 http://www.aboutyun.com/forum.php?mod=forumdisplay&fid=164&page=1 www.aboutyun.com 0 551 1910 171"
    input match {
      case ddPat() => println("a")
      case bkPat() => println("b")
      case ztPat() => println("c")
      case _ => println("!")
    }
    println(ddPat.findAllMatchIn(input).mkString(""))
  }
}