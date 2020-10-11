package com.atguigu.networkflowanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


object UniqueVisitWithBloom {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source = env.readTextFile("/UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = source.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    dataStream
      .filter("pv" == _.behavior)
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger)
      .process(new UvCountResultWithBloom)
  }
}

class MyTrigger extends Trigger[(String, Int), TimeWindow] {
  //每来一条数据触发一次调用
  override def onElement(t: (String, Int), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = ???

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = ???

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = ???
}

//收集齐数据才会调用 正因为这个所以上面才会使用trigger
class UvCountResultWithBloom extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  lazy val jedis: Jedis = new Jedis("localhost")

  // 64Mb
  val bloomFilter = new Bloom(1 << 29)

  val uvCountMap = "uvCountMap"

  override def process(key: String, context: Context, input: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    val currentKey = context.window.getEnd.toString

    val storedBitMapKey = context.window.getEnd.toString
    val count = jedis.hget(uvCountMap, currentKey).toLong
    val userId = input.last._1
    val offset = bloomFilter.hash(userId, 26)
    if (count != null) {
      if (!jedis.getbit(storedBitMapKey, offset)) {
        jedis.setbit(storedBitMapKey, offset, true)
        jedis.hset(uvCountMap, currentKey, (count + 1).toString)
      }
    } else {
      jedis.setbit(storedBitMapKey, offset, true)
      jedis.hset(uvCountMap, currentKey, 1.toString)
    }
  }
}

//cap 是2^n
class Bloom(val cap: Int) extends Serializable {

  def hash(value: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }
}