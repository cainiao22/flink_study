package com.atguigu.networkflowanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ：yanpengfei
 * @date ：2020/10/6 4:12 下午
 * @description
 */


//输入数据样例
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)


object PageView {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("UserBehavior.csv")
    val source = env.readTextFile(resource.getPath)

    val dataStream: DataStream[UserBehavior] = source.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)


    val pvStream = dataStream.filter(_.behavior == "pv")
      .map(_ => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate()

  }

}
