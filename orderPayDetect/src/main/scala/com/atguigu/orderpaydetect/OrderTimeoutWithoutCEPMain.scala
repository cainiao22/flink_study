package com.atguigu.orderpaydetect

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


case class OrderPayEvent(orderId: Long, event: String, txId: Long, timestamp: Long)

case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeoutWithoutCEPMain {

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

    val orderResultStream = source.process(new OrderPayMatchFunction)

    orderResultStream.print("pay")
    orderResultStream.getSideOutput(new OutputTag[OrderResult]("order_timeout")).print("timeout")
  }

}

class OrderPayMatchFunction extends KeyedProcessFunction[Long, OrderPayEvent, OrderResult] {

  lazy val createFlag = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("createFlag", classOf[Boolean]))
  lazy val payFlag = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("payFlag", classOf[Boolean]))
  lazy val timerTsStat = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTsStat", classOf[Long]))

  lazy val ordertimeoutOutputTag = new OutputTag[OrderResult]("ordertimeoutOutputTag")

  override def processElement(value: OrderPayEvent, ctx: KeyedProcessFunction[Long, OrderPayEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    val isPayed = payFlag.value()
    val isCreated = createFlag.value()
    if (value.event == "create") {
      if (isPayed) {
        out.collect(OrderResult(value.orderId, "pay success"))
        payFlag.clear()
        ctx.timerService().deleteEventTimeTimer(timerTsStat.value())
      } else {
        createFlag.update(true)
        ctx.timerService().registerEventTimeTimer(value.timestamp + Time.minutes(15).getSize)
        timerTsStat.update(value.timestamp + Time.minutes(15).getSize)
      }
    } else if (value.event == "pay") {
      if (isCreated) {
        out.collect(OrderResult(value.orderId, "pay success"))
        createFlag.clear()
        ctx.timerService().deleteEventTimeTimer(timerTsStat.value())
      } else {
        payFlag.update(true)
        //支付事件来了，但是订单创建事件没来，watermark还没涨到支付时间
        ctx.timerService().registerEventTimeTimer(value.timestamp)
        timerTsStat.update(value.timestamp + Time.minutes(15).getSize)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderPayEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    if (payFlag.value() && !createFlag.value()) {
      ctx.output(ordertimeoutOutputTag, OrderResult(ctx.getCurrentKey, "payed but not found create"))
      payFlag.clear()
    } else if (!payFlag.value() && createFlag.value()) {
      ctx.output(ordertimeoutOutputTag, OrderResult(ctx.getCurrentKey, "create but not payed"))
      createFlag.clear()
    }

    timerTsStat.clear()
  }
}
