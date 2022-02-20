package com.altman.datastream;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created on 2021/11/27 11:10
 * Copyright (c) 2021/11/27,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class ProcessFunctionDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> source = env.addSource(new SensorSource());
        source.map(value -> value.getTemperature()).print();

        env.execute();
    }
}
