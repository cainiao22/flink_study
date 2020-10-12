package com.atguigu.market.analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdClickCountByProvince(province: String, cnt: Long, windowEnd: String)

case class BlackListUserWarning(userId: Long, adId: Long, msg: String)

object AdClickAnalysis {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource = getClass.getResource("ad_click_log.txt")
    val stream = env.readTextFile(resource.getPath)
      .map(line => {
        val arr = line.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
        //这个东西默认延迟一毫秒
      }).assignAscendingTimestamps(_.timestamp)
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUserResult(100))
      .keyBy(_.province)
      .timeWindow(Time.days(1), Time.seconds(5))

      .aggregate(new AdAggCountFunction, new AddCountFunction)

    stream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("warning")
    stream.print("ad_click_log")

    env.execute("AdClickAnalysis")
  }

}

class FilterBlackListUserResult(threshold: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

  lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val resetTimerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTimerState", classOf[Long]))
  lazy val isBlackState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isBlackState", classOf[Boolean]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    val curCount = countState.value()
    //第一个数据 注册定时器清空状态
    if (curCount == 0) {
      //有可能是伦敦时间
      val ts = (ctx.timerService().currentProcessingTime() / (Time.days(1).getSize) + 1) * Time.days(1).getSize - Time.hours(8).getSize
      resetTimerState.update(ts)
      ctx.timerService().registerEventTimeTimer(ts)
    }
    if (curCount >= threshold) {
      if (!isBlackState.value()) {
        isBlackState.update(true)
        ctx.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(value.userId, value.adId, ""))
      }
      return
    }
    countState.update(curCount + 1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if (timestamp == resetTimerState.value()) {
      isBlackState.clear()
      countState.clear()
    }
  }
}

//IN, ACC, OUT
class AdAggCountFunction extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//IN, OUT, KEY, W <: Window
class AddCountFunction extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    out.collect(
      AdClickCountByProvince(key, input.head, window.getEnd.toString)
    )
  }
}