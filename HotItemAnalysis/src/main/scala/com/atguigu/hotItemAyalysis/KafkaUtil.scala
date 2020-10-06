package com.atguigu.hotItemAyalysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
 * @author ：yanpengfei
 * @date ：2020/10/6 10:25 上午
 * @description：kafka生产数据
 */
object KafkaUtil {

  def writeToKafka(topic: String): Unit = {
    //kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.server", "127.0.0.1:9092")
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](properties)
    val bufferSource = io.Source.fromFile("")
    for (line <- bufferSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
  }

}
