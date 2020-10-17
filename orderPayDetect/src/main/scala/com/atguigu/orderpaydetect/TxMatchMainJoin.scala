package com.atguigu.orderpaydetect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class ReceiptEvent(txId: Long, payChannel: String, timestamp: Long)


object TxMatchMain {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)

  val orderSource = env.readTextFile("order.csv")
    .map(line => {
      val arr = line.split(",")
      OrderPayEvent(arr(0).toLong, arr(1), arr(2).toLong, arr(3).toLong)
    }).assignAscendingTimestamps(_.timestamp)
    .keyBy(_.txId)


  val receiptSource = env.readTextFile("reveiptLog.csv")
    .map(line => {
      val arr = line.split(",")
      ReceiptEvent(arr(0).toLong, arr(1), arr(2).toLong)
    }).assignAscendingTimestamps(_.timestamp)
    .keyBy(_.txId)

  val joinedStream = orderSource.intervalJoin(receiptSource).between(Time.seconds(-3),
    Time.seconds(5))
    .process(new JoinProcessFunction)

  env.execute("TxMatchMain")
}

class JoinProcessFunction extends ProcessJoinFunction[OrderPayEvent, ReceiptEvent, (OrderPayEvent, ReceiptEvent)] {
  override def processElement(left: OrderPayEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderPayEvent, ReceiptEvent, (OrderPayEvent, ReceiptEvent)]#Context, out: Collector[(OrderPayEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}
