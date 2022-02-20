package com.altman.window;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created on 2021/12/4 10:13
 * Copyright (c) 2021/12/4,一个很有个性的名字 所有
 * 每个元素都抽取其中的事件时间，然后判断时间是否是递增的，如果是就发出水位线，否则不发出，
 * 此方法不会周期性的提供水位线
 * @author : Altman
 */
public class MyWatermarkGenerator implements WatermarkGenerator<Tuple3<String,Long,Integer>> {

    private long maxTimestamp = Long.MIN_VALUE;
    @Override
    public void onEvent(Tuple3<String, Long, Integer> event, long eventTimestamp, WatermarkOutput output) {
        long oldTimestamp = maxTimestamp ;
        maxTimestamp = Math.max(maxTimestamp,eventTimestamp);
        if (oldTimestamp < maxTimestamp){
            output.emitWatermark(new Watermark(maxTimestamp));
        }
    }
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

    }
}
