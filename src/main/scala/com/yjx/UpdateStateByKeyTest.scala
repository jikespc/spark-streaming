package com.yjx

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("streaming")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    streamingContext.checkpoint("src/main/checkpoint")
    val receive = streamingContext.socketTextStream("localhost", 9999)
    val result = receive.flatMap(_.split("\\s")).map((_, 1))
    /**
     * updateStateByKey 可以维持每个键的任何状态数据
     * 使用到updateStateByKey要开启checkpoint机制和功能
     */
    val value = result.updateStateByKey((seq: Seq[Int], option: Option[String]) => {
      //首先获取历史数据(如果没有默认为0)
      //seq 之前的数据
      //option 数据的状态
      var count = option.getOrElse(0)
      //计算
     /* for (elem <- seq) {
        count += elem
      }*/
      val value1 = count + "test"
      Option(value1)
    })
    //val value1 = value.map(ele => (ele._1, ele._2 + 1))
    value.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
