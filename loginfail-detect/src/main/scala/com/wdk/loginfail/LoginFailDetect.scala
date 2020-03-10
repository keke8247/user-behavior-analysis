package com.wdk.loginfail

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.kafka.common.metrics.stats.Max

import scala.collection.mutable.ListBuffer

/**
  * @Description:
  *              通过采集用户登录日志
  *              监控在短时间内 登录失败一定次数的异常账户信息
  * @Author:wang_dk
  * @Date:2020 /3/10 0010 22:05
  * @Version: v1.0
  **/
object LoginFailDetect {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        val resourceFile = getClass.getResource("/LoginLog.csv")

        val inputStream = env.readTextFile(resourceFile.getPath)
                .map(line =>{   //拆分数据转换数据格式
                    val attr = line.split(",")
                    LoginEvent(attr(0).trim.toLong,attr(1).trim,attr(2).trim,attr(3).trim.toLong)
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) { //设置5秒的watermark
            override def extractTimestamp(element: LoginEvent): Long = element.timeStamp*1000
        })

        val warningStream =  inputStream.keyBy(_.userId)
                .process(new LoginFailProcess(2,5))

        warningStream.print()

        env.execute("login fail process....")

    }

}

class LoginFailProcess(maxFaileds: Int,timeStep : Long) extends KeyedProcessFunction[Long,LoginEvent,WarningInfo] {
    //定义一个ListState 存储登录失败的数据
    lazy val loginFailState : ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail",classOf[LoginEvent]))


    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, WarningInfo]#Context, out: Collector[WarningInfo]) = {

        if(value.eventType == "fail"){  //登录失败
            //判断是否是第一次失败
            if(!loginFailState.get().iterator().hasNext){    //没有数据  第一次失败
                //注册定时器
                ctx.timerService().registerEventTimeTimer(value.timeStamp*1000 + timeStep*1000)
            }else{

            }

            loginFailState.add(value)
        }else{  //登录成功
            loginFailState.clear()
        }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, WarningInfo]#OnTimerContext, out: Collector[WarningInfo]) = {
        //取出登录失败数据
        val loginFailedList = new ListBuffer[LoginEvent]

        val iter = loginFailState.get().iterator()

        while (iter.hasNext){
            loginFailedList += iter.next()
        }

        //判断 失败次数
        if(loginFailedList.length >= maxFaileds){ //登录失败次数超过max
            out.collect(WarningInfo(ctx.getCurrentKey,loginFailedList.head.timeStamp,loginFailedList.last.timeStamp,"登录失败次数"+loginFailedList.length+"....关小黑屋"))
        }

        loginFailState.clear()
    }
}



case class LoginEvent(userId:Long,ip:String,eventType:String,timeStamp:Long)

case class WarningInfo(userId:Long,startTime:Long,endTime:Long,warningMsg:String)