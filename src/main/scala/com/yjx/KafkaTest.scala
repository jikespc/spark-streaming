package com.yjx

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.{SparkConf, streaming}

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("stream")
    val streamingContext = new StreamingContext(sparkConf, streaming.Seconds(5))

    //配置信息
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node01:9092,node02:9092,node03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "yjx_kafka",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    val topics = Array("spark")
    //创建kafka
    val linesDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils createDirectStream(streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    //打印数据
    linesDStream.map(_.value()).foreachRDD(rdd=>rdd.foreach(println))

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
