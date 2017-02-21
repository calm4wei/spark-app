package feng.wei

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fengwei on 17/2/20.
  */
object UrlCount {

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
    val rdd4 = rdd3.groupBy(_._1).mapValues(
      iter =>
        iter.toList.sortBy(_._2._2).reverse.take(3)
    )
    println(rdd4.collect().toBuffer)

    sc.stop()
  }

}
