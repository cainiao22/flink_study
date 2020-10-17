package com.atguigu.orderpaydetect

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)


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
      ReceiptEvent(arr(0), arr(1), arr(2).toLong)
    }).assignAscendingTimestamps(_.timestamp)
    .keyBy(_.txId)

  val connectedStream = orderSource.connect(receiptSource)

  val resultStream = connectedStream.process(new TxMatchFunction)
  resultStream.print("TxMatchMain")
  resultStream.getSideOutput(new OutputTag[OrderPayEvent]("unmatch_OrderPayEvent")).print("unmatch_OrderPayEvent")
  env.execute("TxMatchMain")
}

//K, IN1, IN2, OUT
class TxMatchFunction extends CoProcessFunction[OrderPayEvent, ReceiptEvent, (OrderPayEvent, ReceiptEvent)] {

  lazy val payEventState = getRuntimeContext.getState(new ValueStateDescriptor[OrderPayEvent]("payEventState", classOf[OrderPayEvent]))
  lazy val receiptEventState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receiptEventState", classOf[ReceiptEvent]))
  val unMatchOrderPayEvent = new OutputTag[OrderPayEvent]("unmatch_OrderPayEvent")
  val unMatchReceiptEvent = new OutputTag[ReceiptEvent]("unMatchReceiptEvent")

  override def processElement1(value: OrderPayEvent, ctx: CoProcessFunction[OrderPayEvent, ReceiptEvent, (OrderPayEvent, ReceiptEvent)]#Context, out: Collector[(OrderPayEvent, ReceiptEvent)]): Unit = {
    if (receiptEventState.value() != null) {
      out.collect((value, receiptEventState.value()))
      receiptEventState.clear()
    } else {
      payEventState.update(value)
      ctx.timerService().registerEventTimeTimer(value.timestamp + 3000)
    }
  }

  override def processElement2(value: ReceiptEvent, ctx: CoProcessFunction[OrderPayEvent, ReceiptEvent, (OrderPayEvent, ReceiptEvent)]#Context, out: Collector[(OrderPayEvent, ReceiptEvent)]): Unit = {
    if (payEventState.value() != null) {
      out.collect((payEventState.value(), value))
      payEventState.clear()
    } else {
      receiptEventState.update(value)
      ctx.timerService().registerEventTimeTimer(value.timestamp)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderPayEvent, ReceiptEvent, (OrderPayEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderPayEvent, ReceiptEvent)]): Unit = {
    if (payEventState.value() != null) {
      ctx.output(unMatchOrderPayEvent, payEventState.value())
    } else if (receiptEventState.value() != null) {
      ctx.output(unMatchReceiptEvent, receiptEventState.value())
    }
  }
}