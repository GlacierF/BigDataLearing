package com.altman.datastream;

import com.altman.util.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 如果某传感器的温度在一秒内（处理时间）持续增加，则发出警告
 */
public class TempIncreaseAlertFunction extends KeyedProcessFunction<String, SensorReading, String> {
    // 存储最近一次传感器的读数
    ValueState<Double> lastTempState;
    // 存储当前活动计时器的时间戳
    ValueState<Long> currentTimerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE()));
        currentTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("currentTimer", Types.LONG()));
    }

    @Override
    public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
        // 获取上一次记录的温度
        Double prevTemp = lastTempState.value();
        //将这次的温度更新进状态中
        lastTempState.update(value.getTemperature());
        //获取当前活动计时器的时间戳
        Long curTimerTimestamp = currentTimerState.value();
        if (prevTemp == null || value.getTemperature() < prevTemp) {
            //温度下降了，删除当前计时器
            if (curTimerTimestamp != null){
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
            }
            //清空状态保存的时间戳
            currentTimerState.clear();
        } else if (value.getTemperature() > prevTemp && curTimerTimestamp == null) {
            //温度上升了且没有设置计时器
            // 以当前时间 +1 秒设置处理时间计时器
            long processingTime = ctx.timerService().currentProcessingTime() + 1000;
            ctx.timerService().registerProcessingTimeTimer(processingTime);
            //状态记录注册的计时器的时间戳
            currentTimerState.update(processingTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect("传感器：" + ctx.getCurrentKey() + "的记录的温度在一秒内持续增加！");
        //计时器触发之后，清空状态保存的计时器时间戳
        currentTimerState.clear();
    }
}
