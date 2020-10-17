package com.atguigu.orderpaydetect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


case class OrderPayEvent(orderId: Long, event: String, txId: Long, timestamp: Long)

case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeoutMain {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("order.csv")
    val source = env.readTextFile(resource.getPath)
      .map(line => {
        val arr = line.split(",")
        OrderPayEvent(arr(0).toLong, arr(1), arr(2).toLong, arr(3).toLong)
      }).assignAscendingTimestamps(_.timestamp)
      .keyBy(_.orderId)

    val orderPattern = Pattern.begin[OrderPayEvent]("create")
      .where(_.event == "create")
      .followedBy("pay").where(_.event == "pay")
      .within(Time.milliseconds(15))

    val outputTag = new OutputTag[OrderResult]("pay_timeout")
    val patternStream = CEP.pattern(source, orderPattern).select(outputTag, new OrderTimeoutSelectFunc, new OrderPaySelectFunc)
    patternStream.print("pay")
    patternStream.getSideOutput(outputTag).print("unpay")

    env.execute()
  }
}


class OrderTimeoutSelectFunc extends PatternTimeoutFunction[OrderPayEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderPayEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("unpay").get(0).orderId
    OrderResult(timeoutOrderId, s"pay timeout $l")
  }
}

class OrderPaySelectFunc extends PatternSelectFunction[OrderPayEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderPayEvent]]): OrderResult = {
    val orderId = map.get("pay").get(0).orderId
    OrderResult(orderId, s"pay")
  }
}