package com.atguigu.networkflowanalysis

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @author ：yanpengfei
 * @date ：2020/10/6 4:12 下午
 * @description
 */


//输入数据样例
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)


case class PvResult(timestamp: Long, cnt: Long)

object PageView {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("UserBehavior.csv")
    val source = env.readTextFile(resource.getPath)

    val dataStream: DataStream[UserBehavior] = source.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)


    val pvStream = dataStream.filter(_.behavior == "pv")
      .map(_ => ("pv", 1L))
      .keyBy(_ => {
        Random.nextInt(10)
      })
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new ConutAggFunction, new CountPvWindowFunction)
     /*
      没来一个加一次
     .keyBy(_.timestamp)
      .sum("cnt")
      */
      //如果不keyBy的话，没来一个值都会重新计算一下
      .keyBy(_.timestamp)
      .process(new ProcessPvAggFunction)

    pvStream.print("pv")

    env.execute("pageView")
  }

}


class ConutAggFunction extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


class CountPvWindowFunction extends WindowFunction[Long, PvResult, Int, TimeWindow] {
  override def apply(key: Int, window: TimeWindow, input: Iterable[Long], out: Collector[PvResult]): Unit = {
    out.collect(PvResult(window.getEnd, input.iterator.next()))
  }
}


class ProcessPvAggFunction extends KeyedProcessFunction[Long, PvResult, Long] {

  lazy val count = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cnt", classOf[Long]))

  override def processElement(value: PvResult, ctx: KeyedProcessFunction[Long, PvResult, Long]#Context, collector: Collector[Long]): Unit = {
    count.update(count.value() + value.cnt)
    ctx.timerService().registerEventTimeTimer(value.timestamp + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvResult, Long]#OnTimerContext, out: Collector[Long]): Unit = {
    out.collect(count.value())
    this.count.clear()
  }
}