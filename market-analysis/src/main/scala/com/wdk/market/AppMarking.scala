package com.wdk.market

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Description:
  *              统计总的App行为
  * @Author:wang_dk
  * @Date:2020 /3/8 0008 16:46
  * @Version: v1.0
  **/
object AppMarking {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //自定义数据源
        val inputDataStream = env.addSource(new APPMarketDataSource())
                .assignAscendingTimestamps(_.timestamp)
                .filter(_.behavior != "UNINSTALL")  //统计推广成果 暂不考虑卸载量
                .map(item => ("dummyKey",1L))
                .keyBy(_._1)
                .timeWindow(Time.hours(1),Time.seconds(10))
                .aggregate(new CountAgg(),new AppMarkingWindowResult())


        inputDataStream.print()

        env.execute("app market channel analysis")
    }
}

//预聚合
class CountAgg() extends AggregateFunction[(String,Long),Long,Long] {
    override def add(value: (String, Long), accumulator: Long) = accumulator+1

    override def createAccumulator() = 0L

    override def getResult(accumulator: Long) = accumulator

    override def merge(a: Long, b: Long) = a+b
}


class AppMarkingWindowResult extends WindowFunction[Long,MarketingViewCount,String,TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
        val start = new Timestamp(window.getStart).toString
        val end = new Timestamp(window.getEnd).toString
        val count = input.iterator.next()

        out.collect(MarketingViewCount(start,end,"appMarket","total",count))
    }
}