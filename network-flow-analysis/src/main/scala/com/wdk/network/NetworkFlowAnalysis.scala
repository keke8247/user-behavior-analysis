package com.wdk.network

import java.lang
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Description:
  *             基于web日志的热门页面统计
  * @Author:wang_dk
  * @Date:2020 /3/7 0007 10:25
  * @Version: v1.0
  **/

//日志样例类
case class LogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//窗口聚合结果样例类
case class UrlViewCount(url : String,windowEnd : Long,count : Long )

object NetworkFlowAnalysis {

    def main(args: Array[String]): Unit = {
        //执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)   //设置并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   //设置时间语义 时间发生时间

        //转换时间格式
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")

        //获取数据
        val dataStream = env.readTextFile("D:\\files\\program\\idea\\user-behavior-analysis\\network-flow-analysis\\src\\main\\resources")
                .map(line => {
                    val attr = line.split(" ")

                    val timestamp = simpleDateFormat.parse(attr(3).trim).getTime

                    LogEvent(attr(0).trim,attr(1).trim,timestamp,attr(5).trim,attr(6).trim)
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LogEvent](Time.seconds(1)) { //最大乱序1秒
                    override def extractTimestamp(element: LogEvent): Long = element.eventTime
                })

        val processDs = dataStream.filter(!_.url.contains("favicon.ico"))
                .keyBy(_.url)
                .timeWindow(Time.minutes(10),Time.seconds(5))
                .allowedLateness(Time.minutes(1))   //allowedLateness 允许最大迟到时间, 水位线为延迟1秒 表示过了水位线之后 窗口关闭 这时候在 watermark < end-of-window + allowedLateness的数据到达 会再次触发窗口统计
                .aggregate(new UrlCountAgg(), new UrlWindowResult()) //预聚合
                .keyBy(_.windowEnd) //根据窗口结束时间分组
                .process(new TopNUrl(5)) //窗口周期内的数据排序输出top5

        processDs.print("hotUrls>")

        env.execute("NetworkFlowAnalysis")
    }

}

//集成累加器函数 AggregateFunction[传入类型,累加器类型,返回类型]
class UrlCountAgg extends AggregateFunction[LogEvent,Long,Long] {
    override def add(value: LogEvent, accumulator: Long) = accumulator+1

    override def createAccumulator() = 0L

    override def getResult(accumulator: Long) = accumulator

    override def merge(a: Long, b: Long) = a + b
}

//返回窗口聚合结果
class UrlWindowResult extends WindowFunction[Long,UrlViewCount,String,TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
        out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
    }
}

class TopNUrl(topSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String] {
    //定义一个列表状态  保留窗口周期内的数据 用于做排序  lazy 只能修饰 val类型的
    lazy val urlsState : ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("usrs-state",classOf[UrlViewCount]))

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]) = {
        //把数据放入状态列表
        urlsState.add(value)

        //注册Timer 延迟10毫秒触发
        ctx.timerService().registerEventTimeTimer(value.windowEnd+10)
    }

    //定时触发事件
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]) = {
        //取出State中的数据
        val urls = urlsState.get()

        //清空状态
        urlsState.clear()

        import scala.collection.JavaConversions._

        val sortedUrls = urls.toList.sortWith(_.count>_.count).take(topSize)

        //定义输出
        val result = new StringBuilder

        result.append("时间:"+ new Timestamp(timestamp-10)+" \n")

        sortedUrls.foreach{
            item => {
                result.append("URL: "+ item.url+" 访问量: "+item.count +"\n")
            }
        }

        result.append("================================")

        TimeUnit.SECONDS.sleep(1)

        //输出结果
        out.collect(result.toString())
    }
}

