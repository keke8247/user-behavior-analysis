package com.wdk.network

import java.lang
import java.sql.Timestamp

import com.wdk.network.UniqueVisitor.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * @Description:
  *              基于布隆过滤器的UV统计
  *              所有数据 放内存中处理 容易撑爆内存.
  *              考虑 压缩数据大小 使用布隆过滤器 位图
  * @Author:wang_dk
  * @Date:2020 /3/8 0008 11:27
  * @Version: v1.0
  **/
object UniqueVisitorWithBloom {

    def main(args: Array[String]): Unit = {
        //创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)   //设置并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   //设置时间语义

        val resources = getClass.getResource("/UserBehavior.csv")

        println("start-------------------> :"+new Timestamp(System.currentTimeMillis()))

        val dataStream = env.readTextFile(resources.getFile)
                .map(line => {
                    val attr = line.split(",")
                    //转换类型
                    UserBehavior(attr(0).trim.toLong,attr(1).trim.toLong,attr(2).trim.toInt,attr(3).trim,attr(4).trim.toLong)
                })
                .assignAscendingTimestamps(_.timestamp*1000) //抽取时间戳.由于数据是按时间正序排好序的  所以直接指定时间戳就好 不需要watermark

        val uvCounts = dataStream
                .filter(_.behavior == "pv")
                .map(item => ("dummyKey",item.userId))
                .keyBy(_._1)
                .timeWindow(Time.hours(1))
                .trigger(new UvTrigger())   //自定义窗口触发操作.如果等到窗口关闭再处理数据 窗口周期内的数据还是在内存中 已然会爆
                .process(new UvCountWindowFucntion())

        uvCounts.print("UV with bloom>")

        env.execute("UV job with bloom")

        println("end-------------------> :"+new Timestamp(System.currentTimeMillis()))
    }

}


//自定义窗口触发器
class UvTrigger() extends Trigger[(String,Long),TimeWindow] {
    var idSet :Set[Long]= Set[Long]()

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = TriggerResult.CONTINUE

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext) = {}

    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = {
        // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态  这样下游操作会频繁进行  性能会有损失.
        TriggerResult.FIRE_AND_PURGE

        //TODO 优化性能
    }
}


//自定义布隆过滤器
class Bloom(size : Long) extends Serializable{
    //定义布隆过滤器位图大小 默认16M  1B = 8位 是2的三次方  16M = 1B * 1024 * 1024 * 16 = 2^27
    val cap = if(size>0) size else 1 << 27

    //定义hash函数  seed 是种子(一般取一个质数)
     def hash(key : String,seed : Int) :Long ={
         var result = 0L
         for (i <- 0 until(key.length)){
             result = result*seed + key.charAt(i)
         }

         //取后位图大小的位数
         result & (cap - 1)
     }
}

class  UvCountWindowFucntion extends ProcessWindowFunction[(String,Long),UvCountWithBloom,String,TimeWindow] {
    //定义redis连接
    lazy val jedis = new Jedis("localhost", 6379)
    lazy val bloom = new Bloom(1<<29) //给一个64M大小的位图

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCountWithBloom]): Unit = {
        //获取windowEnd
        val sortKey = context.window.getEnd.toString
        var counts = 0L

        //把每个窗口的count值(uv) 存入redis(由于trigger里面已经把状态清掉了),
        //存储结构为 HashTable 存入 redis的counts 表 结构是(windowEnd,counts)
        if(jedis.hget("counts",sortKey) != null){   //取出count值
            counts = jedis.hget("counts",sortKey).toLong
        }

        //处理数据
        val userId = elements.last._2.toString  //获取UserId

        val offset = bloom.hash(userId,97)  //获取hash

        //判断该hash在位图中是否存在
        val isExist = jedis.getbit(sortKey,offset)

        if(!isExist){   //不存在
            counts += 1
            jedis.setbit(sortKey,offset,true)
            jedis.hset("counts",sortKey,counts.toString)
//            out.collect(UvCountWithBloom(sortKey,counts))
        }else{
//            out.collect(UvCountWithBloom(sortKey,counts))
        }

    }
}

case class UvCountWithBloom(windowEnd : String, userId : Long)