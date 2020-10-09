package com.atguigu.networkflowanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

import scala.collection.mutable

case class UvCount(windowEnd: Long, cnt: Long)

object UniqueVisit {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source = env.readTextFile("/UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = source.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    dataStream.timeWindowAll(Time.hours(1), Time.minutes(5))
      .apply(new UvCountResult)
  }
}


class UvCountResult extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {

  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //精确实现
    val set = new mutable.HashSet[Long]
    input.iterator.foreach(
      user => set.add(user.userId)
    )

    out.collect(UvCount(window.getEnd, set.size))
  }
}