package com.wdk.order.detect

import com.wdk.order.detect.TxMatchDetect.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020 /3/14 0014 10:36
  * @Version: v1.0
  **/
class TxMatchDetectWithJoin {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //定义inputFile
        val orderFile = getClass.getResource("/OrderLog.csv")

        //1.订单事件输入流
        val inputStream = env.readTextFile(orderFile.getPath)
                .map(line => {
                    val attr = line.split(",")
                    OrderEvent(attr(0).trim.toLong,attr(1).trim,attr(2).trim,attr(3).trim.toLong)
                }).assignAscendingTimestamps(_.timestamp*1000)
                .filter(_.transactionId != "")
                .keyBy(_.transactionId) //根据交易ID 做匹配

        // 收据File
        val receiptFile = getClass.getResource("/ReceiptLog.csv")

        //2.支付到账事件流
        val receiptStream = env.readTextFile(receiptFile.getPath)
                .map(line =>{
                    val attr = line.split(",")
                    ReceiptEvent(attr(0).trim,attr(1).trim,attr(2).trim.toLong)
                })
                .keyBy(_.transactionId) //根据交易ID 做匹配

        val joinedStream = inputStream.join(receiptStream).where(_.transactionId).equalTo(_.transactionId).

    }

}
