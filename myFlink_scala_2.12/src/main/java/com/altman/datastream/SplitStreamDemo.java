package com.altman.datastream;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created on 2021/11/24 7:59
 * Copyright (c) 2021/11/24,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class SplitStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, String>> inputStream = env.fromElements(Tuple2.of(1, "altman"), Tuple2.of(2, "bonnie"),
                Tuple2.of(11, "candy"), Tuple2.of(12, "dennis"));
        DataStream<Tuple2<Integer, String>> tuple2DataStream = inputStream.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                if (key < 10){
                    return 0;
                }else {
                    return 1 ;
                }
            }
        }, new KeySelector<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                return value.f0;
            }
        });
        tuple2DataStream.print();
        env.execute();
    }
}
