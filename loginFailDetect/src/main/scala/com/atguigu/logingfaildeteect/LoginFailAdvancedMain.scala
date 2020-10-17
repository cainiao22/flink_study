package com.atguigu.logingfaildeteect

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LoginFailAdvancedMain {

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


class LoginFailWarningAdvanceProcessFunction extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {

  lazy val loginFailEventListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailEventListState", classOf[LoginEvent]))


  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    if (value.eventType == "failed") {
      //进一步做判断
      if (loginFailEventListState.get().iterator().hasNext) {
        if (loginFailEventListState.get().iterator().next().timestamp + 2 > value.timestamp) {
          out.collect(LoginFailWarning(value.userId, 1L, value.timestamp, ""))
        } else {
          loginFailEventListState.clear()
          loginFailEventListState.add(value)
        }
      } else {
        loginFailEventListState.clear()
      }

    }
  }
}