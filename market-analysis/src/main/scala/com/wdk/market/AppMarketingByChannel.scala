package com.wdk.market


import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * @Description:
  *              分析不同渠道的推广成果
  * @Author:wang_dk
  * @Date:2020 /3/8 0008 15:50
  * @Version: v1.0
  **/
object AppMarketingByChannel {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //自定义数据源
        val inputDataStream = env.addSource(new APPMarketDataSource())
                        .assignAscendingTimestamps(_.timestamp)
                .filter(_.behavior != "UNINSTALL")  //统计推广成果 暂不考虑卸载量
                .map(item=>{
                    ((item.channel,item.behavior),1L)
                }).keyBy(_._1)
                .timeWindow(Time.hours(1),Time.seconds(10))
                .process(new MarketingCountByChannel())

        inputDataStream.print()


        env.execute("app market channel analysis")
    }

}

class APPMarketDataSource() extends SourceFunction[MarketingUserBehavior] {
    var runFlag = true

    override def cancel() = {
        runFlag = false
    }

    //定义 渠道集合
    val channelSeq : Seq[String] = Seq("weibo","wechat","app-store","huawei","wandoujia")

    val behaviorSeq : Seq[String] = Seq("CLICK","DOWNLOAD","INSTALL","UNINSTALL")

    val rand = new Random()

    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]) = {

        // 定义一个生成数据的上限
        val maxElements = Long.MaxValue
        var count = 0L

        // 随机生成所有数据
        while( runFlag && count < maxElements ){
            val id = UUID.randomUUID().toString
            val behavior = behaviorSeq(rand.nextInt(behaviorSeq.size))
            val channel = channelSeq(rand.nextInt(channelSeq.size))
            val ts = System.currentTimeMillis()

            ctx.collect( MarketingUserBehavior( id,channel,behavior, ts ) )

            count += 1
            TimeUnit.MILLISECONDS.sleep(10L)
        }
    }
}

class MarketingCountByChannel extends ProcessWindowFunction[((String,String),Long),MarketingViewCount,(String,String),TimeWindow] {
    override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
        val start = new Timestamp(context.window.getStart).toString
        val end = new Timestamp(context.window.getEnd).toString
        val channel = key._1
        val behavior = key._2
        val count = elements.size

        out.collect(MarketingViewCount(start,end,channel,behavior,count))
    }
}

case class MarketingUserBehavior(userId : String,channel : String, behavior : String, timestamp : Long)

//输出结果样例类
case class MarketingViewCount(start : String,end : String, channel : String , behavior: String,count:Long)