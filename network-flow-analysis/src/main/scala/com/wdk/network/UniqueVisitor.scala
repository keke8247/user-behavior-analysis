package com.wdk.network

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Description:
  *              统计一小时周期内的UV
  * @Author:wang_dk
  * @Date:2020 /3/8 0008 10:22
  * @Version: v1.0
  **/
object UniqueVisitor {
    def main(args: Array[String]): Unit = {
        //创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)   //设置并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   //设置时间语义

        val resources = getClass.getResource("/UserBehavior.csv")

        val dataStream = env.readTextFile(resources.getFile)
                .map(line => {
                    val attr = line.split(",")
                    //转换类型
                    UserBehavior(attr(0).trim.toLong,attr(1).trim.toLong,attr(2).trim.toInt,attr(3).trim,attr(4).trim.toLong)
                })
                .assignAscendingTimestamps(_.timestamp*1000) //抽取时间戳.由于数据是按时间正序排好序的  所以直接指定时间戳就好 不需要watermark

        val uvCounts = dataStream
                .filter(_.behavior=="pv")
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountByWindow())

        uvCounts.print("UV>")

        env.execute("uv job")
    }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
        //定义一个scala set 用于保存窗口周期内的 userId  (set 直接去重)
        var userIdSet: Set[Long] = Set[Long]()

        for(userBehavior <- input){
           userIdSet += userBehavior.userId
        }

        out.collect(UvCount(new Timestamp(window.getEnd),userIdSet.size))

    }
}

case class UvCount(windowEnd : Timestamp, userId : Long)