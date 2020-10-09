package com.atguigu.networkflowanalysis

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author ：yanpengfei
 * @date ：2020/10/6 12:21 下午
 * @description：网络流量分析
 */


case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)


case class PageViewCount(url: String, windowEnd: Long, cnt: Long)

object NetworkFlow {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source = env.readTextFile("/accesslog.txt")
    val dataStream = source.map(line => {
      val arr = line.split(" ")
      val dateformat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      ApacheLogEvent(arr(0), arr(1), dateformat.parse(arr(2)).getTime, arr(3), arr(4))
    })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(64)) {
          override def extractTimestamp(element: ApacheLogEvent): Long = element.timestamp
        })

    val aggStream = dataStream.filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(10))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate(new PageAggFunction, new PageViewCountWindowResult)

    val resultStream = aggStream.keyBy(_.windowEnd)
      .process(new TopNHostPages(3))
  }

}


class PageAggFunction extends AggregateFunction[ApacheLogEvent, Long, Long] {

  override def createAccumulator(): Long = 0

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


class PageViewCountWindowResult extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}


class TopNHostPages(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

  //lazy val pageViewListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("PageViewCount", classOf[PageViewCount]))

  lazy val pageViewCountMapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("PageViewCount", classOf[String], classOf[Long]))

  //上面使用了懒加载 这里可以不用了
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    pageViewCountMapState.put(value.url, value.cnt)
    ctx.timerService().registerEventTimeTimer(value.windowEnd)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //以窗口时间为基准，通过判断偏移量来确定执行哪个定时器功能
    if (ctx.getCurrentKey + 60000L == timestamp) {
      pageViewCountMapState.clear()
      return
    }

    var allPageViewCounts: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]()
    /*val it = pageViewListState.get().iterator()
    while (it.hasNext) {
      allPageViewCounts += it.next()
    }*/
    pageViewCountMapState.entries().forEach(entry => {
      allPageViewCounts += ((entry.getKey, entry.getValue))
    })

    allPageViewCounts.sortWith(_._2 > _._2).take(n)

  }

}