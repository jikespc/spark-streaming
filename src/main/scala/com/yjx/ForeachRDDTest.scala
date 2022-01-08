package com.yjx

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, streaming}

object ForeachRDDTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("stream")
    val streamingContext = new StreamingContext(sparkConf, streaming.Seconds(5))
    //读取数据
    val dStream = streamingContext.socketTextStream("localhost", 9999)
    //转换数据
    val resultDStream = dStream.flatMap(_.split("\\s")).map((_, 1)).reduceByKey(_ + _)
    /**
     * foreachRDD
     * 最基本的输出操作，将func函数应用于DStream中的RDD
     * 上，这个操作会输出数据到外部系统，比如保存RDD到文件
     * 或者网络数据库等。需要注意的是func函数是在运行该
     * streaming应用的Driver进程里执行的
     */
    //将数据输出
    resultDStream.foreachRDD(rdd=>{
      rdd.foreach(ele=>println(ele._1+"---------->"+ele._2))
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
