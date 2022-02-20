package com.altman.datastream;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created on 2021/11/25 7:22
 * Copyright (c) 2021/11/25,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class MyPartitioner implements Partitioner<Tuple2<Integer,String>> {
    @Override
    public int partition(Tuple2<Integer, String> key, int numPartitions) {
        return 0;
    }
}
