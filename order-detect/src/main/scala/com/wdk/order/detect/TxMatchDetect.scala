package com.wdk.order.detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @Description:
  *              实时对账
  * @Author:wang_dk
  * @Date:2020 /3/14 0014 10:03
  * @Version: v1.0
  **/
object TxMatchDetect {

    //定义两个侧输出流 输出对账失败的结果
    val unmatchPay = new OutputTag[OrderEvent]("unmatchPay")
    val unmatchReceipt = new OutputTag[ReceiptEvent]("unmatchReceipt")

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

        //3.把两条事件流合并到一起  做匹配
        val processStream = inputStream.connect(receiptStream)
                .process(new TxMatchFucntion())

        //输出结果
        processStream.print("matched>")
        processStream.getSideOutput(unmatchPay).print("unmatchPay>")
        processStream.getSideOutput(unmatchReceipt).print("unmatchReceipt>")

        env.execute("tx match job")

    }

    class TxMatchFucntion() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)] {
        //定义两个状态  保存先到数据 做匹配
        lazy val payState : ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payState",classOf[OrderEvent]))

        lazy val receiptState : ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receiptState",classOf[ReceiptEvent]))

        override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            //orderEvent 到了 匹配receiptEvent是否到了
            val receipt= receiptState.value()
            if(receipt != null){ //匹配到到账事件  匹配成功
                out.collect((pay,receipt))
                receiptState.clear()
            }else{  //没有匹配到
                payState.update(pay)

                //注册定时器  等待receiptEvent 5秒吧
                ctx.timerService().registerEventTimeTimer(pay.timestamp*1000L + 5*1000L)
            }

        }

        override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            //receiptEvent 到了 匹配OrderEvent是否到了
            val pay = payState.value()
            if(pay!=null){
                out.collect(pay,receipt)
                payState.clear()
            }else{
                receiptState.update(receipt)
                ctx.timerService().registerEventTimeTimer(receipt.timestamp*1000L + 5*1000L)
            }
        }

        override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
            if(payState.value() != null){   //没有匹配到receiptEvent
                ctx.output(unmatchPay,payState.value())
            }
            if(receiptState.value()!= null){
                ctx.output(unmatchReceipt,receiptState.value())
            }

            payState.clear()
            receiptState.clear()
        }
    }

}

case class ReceiptEvent(transactionId:String,payChannel:String,timestamp:Long)