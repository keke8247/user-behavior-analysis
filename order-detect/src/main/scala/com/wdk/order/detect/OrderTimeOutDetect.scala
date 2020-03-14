package com.wdk.order.detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Description:
  *              使用CEP 做订单超时报警处理
  * @Author:wang_dk
  * @Date:2020 /3/12 0012 20:59
  * @Version: v1.0
  **/
object OrderTimeOutDetect {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //定义inputFile
        val inputFile = getClass.getResource("/OrderLog.csv")

        //1.数据输入流
        val inputStream = env.readTextFile(inputFile.getPath)
                .map(line => {
                    val attr = line.split(",")
                    OrderEvent(attr(0).trim.toLong,attr(1).trim,attr(2).trim,attr(3).trim.toLong)
                }).assignAscendingTimestamps(_.timestamp*1000)
                .keyBy(_.orderId)

        //2.定义pattern 订单创建完成后15分钟之内 支付
        val orderEventPattern = Pattern
                .begin[OrderEvent]("begin").where(_.eventType == "create")  //订单创建
                .followedBy("follow").where(_.eventType=="pay")  //订单支付
                .within(Time.minutes(15))       //15分钟之内


        //3.使用pattern
        val orderPatternStream = CEP.pattern(inputStream,orderEventPattern)

        //4.抽取匹配结果
        //4.1 定义一个OutPutTag 存放 没有匹配上的数据
        val orderTimeOutTag = new OutputTag[OrderResult]("orderTimeOut")

        val resultStream = orderPatternStream.select(orderTimeOutTag,new OrderTimeOutMatch(),new OrderSuccessMatch())

        resultStream.print("success order>")
        resultStream.getSideOutput(orderTimeOutTag).print("time out order>")

        env.execute("order time out warning job")

    }

}

class OrderTimeOutMatch() extends PatternTimeoutFunction[OrderEvent,OrderResult] {
    override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long) = {
        val timeoutOrderId = map.get("begin").iterator.next().orderId
        OrderResult(timeoutOrderId,"order time out")
    }
}

class OrderSuccessMatch() extends PatternSelectFunction[OrderEvent,OrderResult] {
    override def select(map: util.Map[String, util.List[OrderEvent]]) = {
        val successOrderId = map.get("follow").iterator.next().orderId
        OrderResult(successOrderId,"order pay success")
    }
}


//定义订单事件样例类
case class OrderEvent(orderId:Long,eventType:String,transactionId:String,timestamp:Long)

//订单支付结果
case class OrderResult(orderId:Long,msg:String)