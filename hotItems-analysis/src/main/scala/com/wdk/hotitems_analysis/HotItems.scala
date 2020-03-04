package com.wdk.hotitems_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Description:
  *              最近一个小时的热门商品的统计 每5分钟统计一次
  * @Author:wang_dk
  * @Date:2020 /3/4 0004 21:13
  * @Version: v1.0
  **/
object HotItems {

    def main(args: Array[String]): Unit = {
        //定义执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)   //设置并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   //设置时间语义为 eventTime

        //source
        val inputStream = env.readTextFile("D:\\files\\program\\idea\\user-behavior-analysis\\hotItems-analysis\\src\\main\\resources\\UserBehavior.csv")
                .map(line=>{
                    val attr = line.split(",")
                    UserBehavior(attr(0).trim.toLong,attr(1).trim.toLong,attr(2).trim.toInt,attr(3).trim,attr(4).trim.toLong)
                })
                .assignAscendingTimestamps(_.timestamp*1000)    // 由于数据是按时间排好序的,所以不需要watermark 只需要指定时间戳

        //转换数据
        val processStream = inputStream
                .filter(_.behavior == "pv")
                .keyBy(_.itemId)
                .timeWindow(Time.hours(1),Time.minutes(5))
                .aggregate(new CountAgg(),new WindowResult())   //按窗口聚合
                .keyBy(_.windowEnd) //  按窗口分组
                .process(new TopN(3))

        processStream.print()

        env.execute("hot items")

    }

}

//预聚合,来一条处理一条
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long] {
    override def add(value: UserBehavior, accumulator: Long) = accumulator+1

    override def createAccumulator() = 0

    override def getResult(accumulator: Long) = accumulator

    override def merge(a: Long, b: Long) = a+b
}

class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow] {
    override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
    }
}

class TopN(topSize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {
    //定义一个ListState 把一个窗口周期内的数据存放起来
    private var listState : ListState[ItemViewCount] = null

    override def open(parameters: Configuration) = {
        listState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("listState",classOf[ItemViewCount]))
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]) = {
        listState.add(value)

        //注册Timeer  延迟1秒触发
        ctx.timerService().registerEventTimeTimer(value.windowEnd+1000)
    }

    // 定时器触发时，对所有数据排序，并输出结果
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]) = {
        import scala.collection.JavaConversions._

        //取出listState里面的数据 按照count排序 取topN
        val sortedList = listState.get().toList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

        listState.clear() //清空状态

        val result = new StringBuilder

        result.append("时间：").append(new Timestamp(timestamp-1000)).append("\n")
        // 输出每一个商品的信息
        for( i <- sortedList.indices ){
            val currentItem = sortedList(i)
            result.append("No").append(i + 1).append(":")
                    .append(" 商品ID=").append(currentItem.itemId)
                    .append(" 浏览量=").append(currentItem.count)
                    .append("\n")
        }
        result.append("================================")
        // 控制输出频率
        Thread.sleep(1000)
        out.collect(result.toString())
    }
}



case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )

// 定义窗口聚合结果样例类
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )
