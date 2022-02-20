package com.altman.window;

import com.altman.util.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 计算窗口内传感器读数的平均值，并向下游发送这个平均值
 * @author : Altman
 */
public class TemperatureAverage implements WindowFunction<SensorReading, Tuple2<String, Double>, String, TimeWindow> {
    @Override
    public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple2<String, Double>> out) throws Exception {
        int count = 0 ;
        double sum = 0 ;
        for (SensorReading sensorReading : input) {
            sum += sensorReading.getTemperature();
            count ++ ;
        }
        double average = sum / count ;
        out.collect(Tuple2.of(sensorId,average));
    }
}
