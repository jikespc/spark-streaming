package com.yjx

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, streaming}

object TransformTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("streaming")
    val streamingContext = new StreamingContext(sparkConf, streaming.Seconds(5))
    val receiver = streamingContext.socketTextStream("localhost", 9999)

    /**
     * transform 通过对源DStream的每RDD应用RDD-to-RDD函数返回一个
     * 新的DStream，这可以用来在DStream做任意RDD操作
     */
    val result = receiver.transform(ele => {
      ele.flatMap(_.split("\\s")).map((_, 1))
    })
    result.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
