package com.atguigu.hotItemAyalysis


import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable.ListBuffer


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

    //kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.server", "127.0.0.1:9092")
    properties.setProperty("group.id", "consumer-hotitems")
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    //读取数据
    val source = env.addSource(new FlinkKafkaConsumer[String]("topic-items", new SimpleStringSchema(), properties))
    //val source = env.readTextFile("/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = source.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    //得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream.filter(_.timestamp == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg, new ItemViewCountWindowResult)

    val resultStream = aggStream
      .keyBy("wondowEnd")
      .process(new TopNHotItems(5))
      .print("resultStream")
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

//这个是窗口结束时候调用的 用于向下输出
class ItemViewCountWindowResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}


//自定义keyedProcessFunction
class TopNHotItems(n: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  //自定义状态
  var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("itemViewCountListState", classOf[ItemViewCount]))
  }

  override def processElement(i: ItemViewCount,
                              context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemViewCountListState.add(i)
    //注册一个定时器  等所有数据都到齐了 输出数据  只有超过watermark之后才会出现
    //这里每来一次数据就会注册一个定时器，但是flink的定时器注册的key值是时间戳，所以重复注册并不会影响
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)

  }

  //定时器触发
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    var allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val it = itemViewCountListState.get().iterator()
    while (it.hasNext) {
      allItemViewCounts += it.next()
    }
    //清空数据
    itemViewCountListState.clear()
    //倒序排
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(n)
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：")
      .append(new Timestamp(timestamp - 1))
      .append("\n")
    for (i <- sortedItemViewCounts.indices) {
      result.append(s"""NO. $i: 商品标号:${sortedItemViewCounts(i).itemId}, 热门度:${sortedItemViewCounts(i).count}""")
        .append("\n")
    }
    result.append("=========================================")
    out.collect(result.toString())
  }
}