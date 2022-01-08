package com.yjx

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, streaming}

object Window {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("stream")
    val streamingContext = new StreamingContext(sparkConf, streaming.Seconds(5))
    val lines = streamingContext.socketTextStream("localhost", 9999)

    /**
     * window 回一个基于源DStream的窗口批次 计算后得到新的DStream
     * 窗口总长度（window length）：你想计算多长时间的数据
     * 滑动时间间隔（slide interval）：你每多长时间去更新一次
     */
    lines.window(streaming.Seconds(20),streaming.Seconds(5)).print()
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
