package com.atguigu.logingfaildeteect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFailMain {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val resource = getClass.getResource("userLoginEvent.csv")
    val stream = env.readTextFile(resource.getPath)
      .map(line => {
        val arr = line.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = Time.seconds(element.timestamp).getSize
      })


    val loginFailPattern = Pattern.begin[LoginEvent]("firstFail").where(_.eventType == "fail")
      .next("secondFail").where(_.eventType == "fail")
      .within(Time.seconds(2))

    val patternStream = CEP.pattern(stream.keyBy(_.userId), loginFailPattern)
    patternStream.select(new LoginFailEventMatchFunction)
  }
}


class LoginFailEventMatchFunction extends PatternSelectFunction[LoginEvent, LoginFailWarning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    val firstFailPattern = map.get("firstFail").get(0)
    val secondFailPattern = map.get("secondFail").get(0)
    LoginFailWarning(firstFailPattern.userId, firstFailPattern.timestamp, secondFailPattern.timestamp, "重复登录失败")
  }
}