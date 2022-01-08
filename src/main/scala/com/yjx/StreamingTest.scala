package com.yjx

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object  StreamingTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("st")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    val receiver = streamingContext.socketTextStream("localhost", 9999)
    val dStream = receiver.flatMap(_.split("\\s")).map((_, 1)).reduceByKey(_ + _)
    dStream.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
