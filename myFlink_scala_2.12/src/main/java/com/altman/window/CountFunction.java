package com.altman.window;

import com.altman.util.SensorReading;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 统计每个传感器在窗格窗口中的读数个数
 * @author : Altman
 */
public class CountFunction extends ProcessWindowFunction<SensorReading,Tuple3<String,Long,Integer>,String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<SensorReading> elements, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
        Integer count = 0 ;
        for (SensorReading element : elements) {
            count ++ ;
        }
        out.collect(Tuple3.of(key,context.currentWatermark(),count));
    }
}