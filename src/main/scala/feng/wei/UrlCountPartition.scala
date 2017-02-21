package feng.wei

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by fengwei on 17/2/20.
  */

object UrlCountPartition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("urlcount").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.textFile("/Users/fengwei/Documents/work/workspace/mywork/SparkApp/data/itcast.log")
      .map { line =>
        val arr = line.split("\t")
        (arr(1), 1)
      }

    val rdd2 = rdd1.reduceByKey(_ + _)
    val rdd3 = rdd2.map { r =>
      val host = new URL(r._1).getHost
      (host, (r._1, r._2))
    }
    val hosts = rdd3.keys.distinct().collect()
    val rdd4 = rdd3.partitionBy(new HostPartitioner(hosts))
    rdd4.saveAsTextFile("/Users/fengwei/Documents/work/workspace/mywork/SparkApp/data/out1")
    val rdd5 = rdd4.mapPartitions(
      r =>
        r.toList.sortBy(_._2._2).reverse.take(3).iterator
    )
    rdd5.saveAsTextFile("/Users/fengwei/Documents/work/workspace/mywork/SparkApp/data/out2")
    rdd5.foreach(println(_))

    sc.stop()

  }
}

class HostPartitioner(ints: Array[String]) extends Partitioner {

  val map = new mutable.HashMap[String, Int]()
  var count = 0
  for (i <- ints) {
    map += (i -> count)
    count += 1
  }


  override def numPartitions: Int = ints.length

  override def getPartition(key: Any): Int = {
    map.getOrElse(key.toString, 0)
  }
}