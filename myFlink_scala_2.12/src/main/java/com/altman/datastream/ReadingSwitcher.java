package com.altman.datastream;

import com.altman.util.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 动态控制数据流是否继续往下游转发，当forwardingEnabled中的值为true时，表示转发是开启的，否则转发是关闭的
 * @author : Altman
 */
public class ReadingSwitcher extends CoProcessFunction<SensorReading, Tuple2<String,Long>,SensorReading> {

    //是否向下游转发数据的开关
    private ValueState<Boolean> forwardingEnabled;
    // 用于保存当前当前活动的 停止计时器的时间戳
    private ValueState<Long> disableTimer;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化状态
        forwardingEnabled = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("forwardingEnabled", Types.BOOLEAN()));
        disableTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("disableTimer", Types.LONG()));
    }

    @Override
    public void processElement1(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
        // 检查是否可以往下游传递数据
        if (forwardingEnabled.value() != null && forwardingEnabled.value()) {
            out.collect(value);
        }
    }

    @Override
    public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
        //开启读数转发
        forwardingEnabled.update(true);

        // 设置停止计时器，待时间到了之后，调用onTimer()方法，将forwardingEnabled设置为false，从而停止转发
        long timerTimestamp = ctx.timerService().currentProcessingTime() + value.f1;
        Long curTimestamp = disableTimer.value();
        if (curTimestamp == null){
            curTimestamp = -1L;
        }
        if (timerTimestamp > curTimestamp) {
            // 删除之前的计时器，重新计时
            ctx.timerService().deleteProcessingTimeTimer(curTimestamp);
            ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
            // 更新当前计时器的时间戳
            disableTimer.update(timerTimestamp);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
        // 将转发状态设置为false,从而实现关闭转发的操作
        forwardingEnabled.update(false);
        disableTimer.clear();
    }
}