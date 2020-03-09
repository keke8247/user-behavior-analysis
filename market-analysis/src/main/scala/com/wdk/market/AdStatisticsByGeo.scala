package com.wdk.market

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Description:
  *              地域 广告 点击分析
  * @Author:wang_dk
  * @Date:2020 /3/9 0009 21:12
  * @Version: v1.0
  **/
object AdStatisticsByGeo {

    val blackListOutPut = OutputTag[BlackList]("blackList")

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        //加载数据
        val filePath = getClass.getResource("/AdClickLog.csv")

        val adClickDataStream = env.readTextFile(filePath.getPath)
                .map(line => {  //切分数据 组装成 AdClickEvent
                    val attr = line.split(",")
                    AdClickEvent(attr(0).trim.toLong,attr(1).trim.toLong,attr(2).trim,attr(3).trim,attr(4).trim.toLong)
                })
                .assignAscendingTimestamps(_.timestamp*1000) //指定时间戳

        //过滤掉 同一个人 重复点击同一条广告超过一定的行为
        val blackFilterDataStream = adClickDataStream
                .keyBy( imem=> (imem.userId,imem.adId)) //根据 userId 和 adId 分组
                .process(new BlackFilterProcessFunction(100))


        val adCountStream = blackFilterDataStream
                .keyBy(_.province)      //根据省份为维度进行统计
                .timeWindow(Time.hours(1),Time.seconds(5))
                .aggregate(new AdCountAgg(),new AdCountWindowResult())



        adCountStream.print("Ad count>")

        blackFilterDataStream.getSideOutput(blackListOutPut).print("BlackList--->")

        env.execute("Ad count statistics")
    }

    class BlackFilterProcessFunction(maxCount: Int) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent] {
        //定义一个状态 记录点击次数
        lazy val countState : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]))

        //是否已经发送到黑名单标志位
        lazy val blackState : ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("black-state",classOf[Boolean]))

        //定义一个状态 记录Timer 触发时间戳
        lazy val timerState : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state",classOf[Long]))

        override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]) :Unit= {
            //获取 count
            val counts = countState.value()

            if(counts == 0){    //第一次点击
                //注册定时器  到时候需要清空状态
                val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) +1) * 1000*60*60*24
                timerState.update(ts)
                ctx.timerService().registerProcessingTimeTimer(ts)
            }
            //如果大于maxCount
            if(counts >= maxCount){
                //判断是否已经发送到黑名单
                if(!blackState.value()){ //如果还没有发送黑名单
                    blackState.update(true)
                    //发送黑名单 到侧输出流
                    ctx.output(blackListOutPut,BlackList(value.userId,value.adId,"点击次数过多,你被拉入黑名单了....."))
                }
                return  //这里如果使用return 关键字 方法返回空的时候一定要写上 :Unit
            }
            countState.update(counts + 1)
            out.collect(value)
        }

        //定时器触发  清空状态
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
            if(timestamp == timerState.value()){    //如果触发定时器的时候 和 定时器时间戳一致 清空状态
                countState.clear()
                blackState.clear()
                timerState.clear()
            }
        }
    }

}


//自定义预聚合累加器
class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long] {
    override def add(value: AdClickEvent, accumulator: Long) = accumulator+1

    override def createAccumulator() = 0L

    override def getResult(accumulator: Long) = accumulator

    override def merge(a: Long, b: Long) = a+b
}

class AdCountWindowResult() extends WindowFunction[Long,AdCountResult,String,TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountResult]): Unit = {
        out.collect(AdCountResult(window.getEnd,key,input.iterator.next()))
    }
}

class ProvinceClickStatisticsFnc() extends KeyedProcessFunction[(Long,String),AdCountResult,String] {
    override def processElement(value: AdCountResult, ctx: KeyedProcessFunction[(Long, String), AdCountResult, String]#Context, out: Collector[String]) = {
        out.collect(new Timestamp(value.windowEnd).toString + value.province + value.count)
    }
}


//广告点击事件
case class AdClickEvent(userId : Long, adId : Long, province : String,city : String, timestamp : Long)

//预聚合窗口返回结果
case class AdCountResult(windowEnd : Long, province : String, count : Long)

//同一个人 高频点击同一条广告 放入黑名单
case class BlackList(userId:Long, adId:Long, msg:String)