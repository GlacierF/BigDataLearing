//package com.altman.chapter5
//
//import com.altman.util.{SensorReading, SensorSource, SensorTimeAssigner}
//import org.apache.flink.api.common.eventtime.{WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
//import org.apache.flink.api.scala.createTypeInformation
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//
///**
// * Created on 2021/11/22 7:40
// * Copyright (c) 2021/11/22,一个很有个性的名字 所有
// *
// * @author : Altman
// */
//object AverageSensorReadings {
//  def main(args: Array[String]): Unit = {
//    // 设置流式执行环境
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    //从应用中使用事件时间,flink 1.12之后默认就是了，这里不再设置
////    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    // 从流式数据源中创建DataStream[SensorReading]对象
//    env.addSource(new SensorSource)
//      .assignTimestampsAndWatermarks()
//}
