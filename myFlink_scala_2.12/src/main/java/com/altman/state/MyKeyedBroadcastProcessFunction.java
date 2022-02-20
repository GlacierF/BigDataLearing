package com.altman.state;

import com.altman.util.SensorReading;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Locale;
import java.util.Map;

/**
 * 广播流中的事件是"sensor_1","sensor_111"等格式，这些就是传感器id值，只有广播流状态中已经有这个传感器id了，
 * 那么这个传感器的读数才能继续向下游传递。
 * @author : Altman
 */
public class MyKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<String, SensorReading,String,SensorReading> {

    /**
     * 注意这个状态描述符必须跟广播流中传递的参数广播状态描述符一致，否则读不到
     */
    private final MapStateDescriptor<String,String> rulesDesc = new MapStateDescriptor<String, String>("rules",String.class,String.class);

    private final MapStateDescriptor<String,String> internalDesc = new MapStateDescriptor<String, String>("internal",String.class,String.class);

    @Override
    public void processElement(SensorReading value, ReadOnlyContext ctx, Collector<SensorReading> out) throws Exception {
        //获取一个只读的广播状态
        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(rulesDesc);
        //符合条件的传感器id才能向下游传递数据
        if (broadcastState.contains(value.getId())){
            out.collect(value);
            getRuntimeContext().getMapState(internalDesc).put(value.getId(),"altman");
        }
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<SensorReading> out) throws Exception {
        // 将规则流中的事件放入广播状态中
        ctx.getBroadcastState(rulesDesc).put(value,value);
        ctx.applyToKeyedState(internalDesc, new KeyedStateFunction<String, MapState<String, String>>() {
            @Override
            public void process(String key, MapState<String, String> state) throws Exception {
                Iterable<Map.Entry<String, String>> entries = state.entries();
                for (Map.Entry<String, String> entry : entries) {
                    entry.setValue(entry.getValue().toUpperCase(Locale.ENGLISH));
                }
                entries.forEach(System.out::println);
            }
        });
    }
}
