package com.altman.state;

import com.altman.util.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 对传感器连续的两个测量值进行检测，如果差值大于一个特定的阈值，那么就发出警报。
 * 并且如果某个传感器在一个小时内都没有新的测量值，那么会将其对应的状态清除，否则会延迟清理时间
 * @author : Altman
 */
public class SelfCleaningTempAlertFunction extends KeyedProcessFunction<String, SensorReading, Tuple3<String,Double,Double>> {

    /**
     * 传感器上一次的温度
     */
    private ValueState<Double> lastTempState;

    /**
     * 传感器上次注册的计时器的触发时间
     */
    private ValueState<Long> lastTimerState ;
    /**
     * 同一个传感器的连续两次温度测量值的温差大于这个阈值，就发出警报
     */
    private final double threshold ;

    public SelfCleaningTempAlertFunction(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp",Double.class));

        lastTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("lastTimer",Long.class));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        //获取当前计时器的时间戳，并删除这个计时器
        Long curTimer = lastTimerState.value();
        if (curTimer != null){
            ctx.timerService().deleteEventTimeTimer(curTimer);
        }
        //注册一个新的计时器，触发时间比当前时间晚一个小时
        long newTimer = ctx.timestamp() + 60 * 60 * 1000L;
        ctx.timerService().registerEventTimeTimer(newTimer);
        //更新计时器时间戳
        lastTimerState.update(newTimer);

        //从状态中获取上一次的温度
        Double lastTemp = lastTempState.value();
        if (lastTemp != null){
            //检查是否要发出警报
            double tempDiff = Math.abs(value.getTemperature() - lastTemp);
            if (tempDiff > threshold){
                //发出警报
                out.collect(Tuple3.of(value.getId(), value.getTemperature(), tempDiff));
            }
        }
        //更新lastTemp的状态
        lastTempState.update(value.getTemperature());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        //执行到这里，说明这个传感器在一个小时内没有新的测量值到来，删除这个传感器原来的状态
        lastTempState.clear();
        lastTimerState.clear();
    }
}
