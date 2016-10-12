package org.fun.task

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by fun on 16/9/27.
  */
object ReceiveKafkaStream {

  val kafkaTopic = "stg-js-collection"

  def main(args: Array[String]): Unit = {

    //kafka brokers
    val brokers = "192.168.40.29:9092"
    //sparkConf
    val sparkConf = new SparkConf()
      .setMaster("spark://192.168.40.21:7077")
      .setAppName("KafkaReciver")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(kafkaTopic))

    messages.foreachRDD(rdd =>
      if(rdd.count > 0) {
        val now:Date = new Date()
        val fm = new SimpleDateFormat("yyyy-MM-dd")
        val nowString = fm.format(now)
        val data = rdd.map(_._2)
        println(data)
        data.saveAsTextFile("hdfs://192.168.40.21:9000/user/collections/" + nowString + '/' + now.getTime())
      } else {
        println(">>>RDD:Empty")
      }
    )
    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }

}
