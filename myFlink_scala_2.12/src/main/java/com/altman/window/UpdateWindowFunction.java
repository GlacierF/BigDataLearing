package com.altman.window;


import com.altman.util.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 用窗口状态保存当前窗口是否是第一次计算，如果是的话，就发出一个{@link Tuple4}对象，并且第四个元素是"firstTime"，
 * 否则就是因为收到了迟到事件，是更新计算，返回的第四个元素是"update"
 * @author : Altman
 */
public class UpdateWindowFunction extends ProcessWindowFunction<SensorReading, Tuple4<String,Long,Integer,String>,String, TimeWindow> {

    private ValueStateDescriptor<Boolean> isUpdateState ;
    @Override
    public void open(Configuration parameters) throws Exception {
        isUpdateState = new ValueStateDescriptor<Boolean>("isUpdate",Boolean.class){};
    }

    @Override
    public void process(String s, Context context, Iterable<SensorReading> elements, Collector<Tuple4<String, Long, Integer, String>> out) throws Exception {
        Integer count = 0  ;
        for (SensorReading element : elements) {
            count ++;
        }
        ValueState<Boolean> state = context.windowState().getState(isUpdateState);
        if (state.value() == null){
            // 首次计算并发出结果
            out.collect(Tuple4.of(s,context.window().getEnd(),count,"firstTime"));
        }else {
            //并非首次计算、发出新结果
            out.collect(Tuple4.of(s,context.window().getEnd(),count,"update"));
        }
    }
}
