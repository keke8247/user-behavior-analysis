package com.wdk.hotitems_analysis

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.io.network.buffer.BufferBuilder
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * @Description:
  *              统计一小时之内的热门商品
  * @Author:wang_dk
  * @Date:2020 /3/5 0005 20:12
  * @Version: v1.0
  **/
object OneHourHotItems {

    def main(args: Array[String]): Unit = {
        //创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)   //设置并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   //设置时间语义

        //从kafka消费数据
        val properties = new Properties()
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092,slave1:9092,slave2:9092")
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"hotItemGroup")
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

        //声明consumer
        val sourceKafka = new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties)

        val dataStream = env.addSource(sourceKafka)
                .map(line => {
                    val attr = line.split(",")
                    //转换类型
                    OneHourUserBehavior(attr(0).trim.toLong,attr(1).trim.toLong,attr(2).trim.toInt,attr(3).trim,attr(4).trim.toLong)
                })
                .assignAscendingTimestamps(_.timestamp*1000) //抽取时间戳.由于数据是按时间正序排好序的  所以直接指定时间戳就好 不需要watermark

        val processedStream = dataStream
                .filter(_.behavior=="pv")   //只统计点击 热门商品
                .keyBy(_.itemId)    //根据ItemId分组
                .timeWindow(Time.hours(1),Time.minutes(5))  //开窗
                .aggregate(new CountAggregate(),new AggWindowFunction())    //预聚合
                .keyBy(_.windowEnd)     //根据窗口分组
                .process(new TopNItems(3))  //统计topN

        processedStream.print()

        env.execute("hot items...")
    }

}

//预聚合
class CountAggregate() extends AggregateFunction[OneHourUserBehavior,Long,Long] {

    //累加 来一条数据 累加器+1
    override def add(value: OneHourUserBehavior, accumulator: Long) = {
        accumulator+1
    }

    //创建累加器
    override def createAccumulator() = 0L

    //返回累加器结果
    override def getResult(accumulator: Long) = {
        accumulator
    }

    //合并累加器结果
    override def merge(a: Long, b: Long) = {
        a+b
    }
}

class AggWindowFunction() extends WindowFunction[Long,OneHourItemViewCount,Long,TimeWindow] {
    override def apply(itemId: Long, window: TimeWindow, input: Iterable[Long], out: Collector[OneHourItemViewCount]): Unit = {
        //把预聚合得到的累加器结果 和 itemId  windowEnd 组装 使用out.collect 返回
        out.collect(OneHourItemViewCount(itemId,window.getEnd,input.iterator.next()))
    }
}

//统计topN  要比较窗口周期内 所有数据 出现的次数 比较ItemViewCount.count
class TopNItems(topSize: Int) extends KeyedProcessFunction[Long,OneHourItemViewCount,String] {
    //1.既然要比较数据 肯定要把数据保存下来 需要声明一个状态ListState 存放每条数据
    private var itemState : ListState[OneHourItemViewCount]=null;

    //2.初始化itemStated
    override def open(parameters: Configuration) = {
        itemState = getRuntimeContext.getListState(new ListStateDescriptor[OneHourItemViewCount]("itemState",classOf[OneHourItemViewCount]))
    }

    //3.每条数据进来之后 处理数据
    override def processElement(value: OneHourItemViewCount, ctx: KeyedProcessFunction[Long, OneHourItemViewCount, String]#Context, out: Collector[String]) = {
        //每条数据进来 先放到itemState里
        itemState.add(value)

        //注册一个定时器 窗口结束后触发定时器 windowEnt+1000ms 延迟1秒等待延迟数据
        ctx.timerService().registerEventTimeTimer(value.windowEnd+1000)
    }

    //4.定时器触发 统计topN
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OneHourItemViewCount, String]#OnTimerContext, out: Collector[String]) = {
        //引入 scala 和java 的 集合转换器
        import scala.collection.JavaConversions._

        //先取出itemState里面的数据
        val items = itemState.get()

        //去topN 根据count排序
        val sortedItems = items
                .toList //把 Iterable转成List 便于使用排序方法
                .sortBy(_.count)(Ordering.Long.reverse) //根据count排序  Ordering.Long.reverse  倒叙排序
                .take(topSize)  //取topN

        //清空状态
        itemState.clear()

        //输出结果
        val result : StringBuilder = new StringBuilder
        result.append("时间:").append(new Timestamp(timestamp-1000)).append("\n")
        for(i <- sortedItems.indices){  //根据sortedItems的下标遍历
            val item = sortedItems(i)
            result.append("No"+(i+1) + ": 商品ID="+item.itemId+" 浏览量="+item.count+"\n")
        }
        result.append("================================")

        //控制输出频率
        TimeUnit.SECONDS.sleep(2)

        out.collect(result.toString())
    }
}

case class OneHourUserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )

// 定义窗口聚合结果样例类
case class OneHourItemViewCount( itemId: Long, windowEnd: Long, count: Long )