package feng.wei

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fengwei on 17/2/21.
  */
object IpLocation {

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ipLocation").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val ipsRDD = sc.textFile("data/ip.txt")
    val ipRulesRDD = ipsRDD.map {
      line =>
        val arr = line.split("\\|")
        val start_ip = arr(2)
        val end_ip = arr(3)
        val province = arr(6)
        (start_ip, end_ip, province)
    }

    val ipRulesBroadcast = sc.broadcast(ipRulesRDD.collect())

    val ipsLogRDD = sc.textFile("data/access_log").map {
      line =>
        val fields = line.split("\\|")
        fields(0)
    }

    val result = ipsLogRDD.map {
      ip =>
        val ipLong = ip2Long(ip)
        val index = binarySearch(ipRulesBroadcast.value, ipLong)
        ipRulesBroadcast.value(index)
    }

    result foreach (println(_))

    sc.stop()

  }
}
