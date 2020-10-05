package com.atguigu.hotItemAyalysis


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//输入数据样例
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//窗口聚合样例
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItems {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //调试用
    env.setParallelism(1)
    //定义事件事件语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //读取数据
    val source = env.readTextFile("/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = source.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    //得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream.filter(_.timestamp == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg, new ItemViewCountWindowResult)

    env.execute(HotItems.getClass.getSimpleName)
  }
}


class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {

  //初始情况 刚创建的时候pv一定是0
  override def createAccumulator(): Long = 0

  //每来一条数据 调用一次
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  //交给windowFunction
  override def getResult(acc: Long): Long = acc

  //窗口合并时候使用 暂时用不到
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


class ItemViewCountWindowResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}