package com.atguigu.market.analysis

import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)


case class MarketViewCountResult(windowStart: Long, windowEnd: Long, channel: String, behavior: String, cnt: Long)

class SimulatedSource extends RichSourceFunction[MarketUserBehavior] {

  var running = true


  val behaviors: Seq[String] = Seq("view", "download", "install", "uninstall")

  val channels: Seq[String] = Seq("appstore", "weibo", "wechart", "tieba")

  val random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    while (running) {
      val userId = UUID.randomUUID().toString
      val behavior = MarketUserBehavior(userId, behaviors(random.nextInt(behaviors.size)), channels(random.nextInt(channels.size)), System.currentTimeMillis())
      ctx.collect(behavior)
      Thread.sleep(300)
    }
  }

  override def cancel(): Unit = {
    this.running = false
  }
}

object MarketAnalysisByChannel {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val source = env.addSource(new SimulatedSource).assignAscendingTimestamps(_.timestamp)
    source.filter(_.behavior == "uninstall")
      .keyBy(data => (data.channel, data.behavior))
      .timeWindow(Time.days(1), Time.seconds(10))
      .process(new MarketCountByWindowProcessFunction)
      .print("MarketViewCount")

    env.execute("MarketAnalysisByChannel")
  }

}

//IN, OUT, KEY, W
class MarketCountByWindowProcessFunction extends ProcessWindowFunction[MarketUserBehavior, MarketViewCountResult, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCountResult]): Unit = {
    val channel = key._1
    val behavior = key._2
    val cnt = elements.size
    out.collect(MarketViewCountResult(context.window.getStart, context.window.getEnd, channel, behavior, cnt))
  }
}