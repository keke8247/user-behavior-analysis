package com.wdk.util

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * @Description:
  *              创建一个 KafkaProducer工具类  往Kafka提交数据
  * @Author:wang_dk
  * @Date:2020 /3/5 0005 21:48
  * @Version: v1.0
  **/
object KafkaProducerUtils {

    def main(args: Array[String]): Unit = {
        val filePath = "D:\\files\\program\\idea\\user-behavior-analysis\\hotItems-analysis\\src\\main\\resources\\UserBehavior.csv";
        writeDataToKafka("hotitems",filePath)
    }


    def writeDataToKafka(topic: String, filePath: String): Unit ={
        val properties = new Properties()
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092")
        properties.put(ProducerConfig.ACKS_CONFIG,"1")
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String,String](properties)

        val bufferedSource = io.Source.fromFile(filePath)
        for(line <- bufferedSource.getLines()){
            val record: ProducerRecord[String, String] = new ProducerRecord[String,String](topic,line)
            producer.send(record)

            TimeUnit.MILLISECONDS.sleep(10)
        }
        producer.close()
    }
}
