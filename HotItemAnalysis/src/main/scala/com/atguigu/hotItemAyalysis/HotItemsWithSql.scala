package com.atguigu.hotItemAyalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._

/**
 * @author ：yanpengfei
 * @date ：2020/10/6 10:33 上午
 * @description：sql方式获取热门商品
 */
object HotItemsWithSql {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source = env.readTextFile("/UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = source.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)


    val tableStream = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    val aggTable = tableStream.filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'ws)
      .groupBy('itemId, 'ws)
      .select('itemId, 'ws.end as 'widowEnd, 'itemId.count as 'cnt)

    tableEnv.createTemporaryView("resultTable", aggTable, 'itemId, 'widowEnd, 'cnt)

    val resultTable = tableEnv.sqlQuery(
      """
        |select * from (
        |select *, row_number()
        | over (partition by windowEnd order by cnt desc)
        | as row_num
        |from
        | resultTable
        |) as a
        |where
        | row_num <= 5
        |""".stripMargin)

    tableEnv.execute("HotItemsWithSql")
  }

}
