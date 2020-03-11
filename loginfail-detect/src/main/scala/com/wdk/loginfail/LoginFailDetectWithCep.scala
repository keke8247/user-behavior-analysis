package com.wdk.loginfail

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Description:
  *              使用 CEP(Complex Event Processing) 复杂事件处理
  *              解决 连续多次登录失败 报警
  *              会自动排序处理 watermark期限内的乱序数据
  * @Author:wang_dk
  * @Date:2020 /3/11 0011 21:02
  * @Version: v1.0
  **/
object LoginFailDetectWithCep {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val resourceFile = getClass.getResource("/LoginLog.csv")

        //输入事件流
        val inputStream = env.readTextFile(resourceFile.getPath)
                .map(line =>{   //拆分数据转换数据格式
                    val attr = line.split(",")
                    LoginEvent(attr(0).trim.toLong,attr(1).trim,attr(2).trim,attr(3).trim.toLong)
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) { //设置5秒的watermark
            override def extractTimestamp(element: LoginEvent): Long = element.timeStamp*1000
        }).keyBy(_.userId)

        //定义两秒内连续登录失败Pattern
        val continuousLoginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType=="fail")
                .next("next").where(_.eventType=="fail").within(Time.seconds(2)).times(2,4)

        //使用pattern
        val patternStream = CEP.pattern(inputStream,continuousLoginFailPattern)

        //提取匹配上的事件序列
        val loginFailStream = patternStream.select(new LoginFailMatch())

        loginFailStream.print()

        env.execute("login fail with CEP job")

    }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent,WarningInfo] {
    override def select(map: util.Map[String, util.List[LoginEvent]]) = {
        //取出匹配到的序列
        val firstFail = map.get("begin").iterator().next()
        val lastFail = map.get("next").iterator().next()

        WarningInfo(firstFail.userId,firstFail.timeStamp,lastFail.timeStamp,"登录失败次数过多.黑屋见....")
    }
}