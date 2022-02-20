package com.altman.window;

import com.altman.util.SensorReading;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 计算每个窗口内的最大最小值
 * @author : Altman
 */
public class HighAndLowTempProcessFunction extends ProcessWindowFunction<SensorReading,MinMaxTemp,String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<SensorReading> elements, Collector<MinMaxTemp> out) throws Exception {
        // 保存最小值
        double min = Double.MAX_VALUE ;
        // 保存最大值
        double max = Double.MIN_VALUE ;
        // 遍历窗口的每个元素，然后获取最大最小值
        for (SensorReading element : elements) {
            if (element.getTemperature() > max){
                max = element.getTemperature() ;
            }
            if(element.getTemperature() < min){
                min = element.getTemperature();
            }
        }
        // 将最大最小值发生出去
        out.collect(new MinMaxTemp(key,min,max,context.window().getEnd()));
    }
}
