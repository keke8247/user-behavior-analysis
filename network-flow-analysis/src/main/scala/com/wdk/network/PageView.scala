package com.wdk.network


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Description:
  * @Author:wang_dk
  * @Date:2020 /3/8 0008 10:03
  * @Version: v1.0
  **/
object PageView {
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

        val paveViewCounts = dataStream
                .filter(_.behavior=="pv")
                .map(item =>("pv",1))
                .keyBy(_._1)
                .timeWindow(Time.hours(1))
                .sum(1)


        paveViewCounts.print()

        env.execute("page view job...")
    }
}

case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )