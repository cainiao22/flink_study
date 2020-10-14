package com.atguigu.logingfaildeteect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFailMain {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val resource = getClass.getResource("userLoginEvent.csv")
    env.readTextFile(resource.getPath)
      .map(line => {
        val arr = line.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = Time.seconds(element.timestamp).getSize
      })
      .keyBy(_.userId)
      .process(new LoginFailWarningProcessFunction(2))

  }
}


class LoginFailWarningProcessFunction(val count: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {

  lazy val loginFailEventListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailEventListState", classOf[LoginEvent]))
  lazy val timeTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeTsState", classOf[Long]))

  //使用窗口实现的话会不太好确定每个元素
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    if (value.eventType == "fail") {
      loginFailEventListState.add(value)
      if (timeTsState.value() == 0) {
        val ts = Time.seconds(value.timestamp + 2).getSize
        ctx.timerService().registerEventTimeTimer(ts)
        timeTsState.update(ts)
      }
    }else{
      loginFailEventListState.clear()
      ctx.timerService().deleteEventTimeTimer(timeTsState.value())
      timeTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    val list = new ListBuffer[LoginEvent]
    val it = loginFailEventListState.get().iterator()
    while (it.hasNext){
      list += it.next()
    }
    if(list.size >= count){
      out.collect(LoginFailWarning(ctx.getCurrentKey, list.head.timestamp, list.last.timestamp, s"失败${list.size}次"))
    }
  }
}