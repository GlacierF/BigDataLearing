package com.altman.window;

import com.altman.util.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 一个可以提前触发窗口计算发出早期结果的触发器
 * @author : Altman
 */
public class OneSecondIntervalTrigger extends Trigger<SensorReading, TimeWindow> {

    private final ValueStateDescriptor<Boolean> firstSeenDesc = new ValueStateDescriptor<Boolean>("firstSeen", Boolean.class);


    @Override
    public TriggerResult onElement(SensorReading element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

        ValueState<Boolean> firstSeen = ctx.getPartitionedState(firstSeenDesc);
        if (firstSeen.value() == null){
            // 将水位线取整到秒，然后计算下一次触发时间
            long nextTime = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
            ctx.registerEventTimeTimer(nextTime);
            //为窗口结束时间注册计时器
            ctx.registerEventTimeTimer(window.maxTimestamp());
            firstSeen.update(true);
        }
        //返回CONTINUE表示到了元素不会触发计算
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        //我们这个是事件时间触发器，处理时间计时器不要触发计算
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp()){
            //进行最终计算，并清除窗口状态
            return TriggerResult.FIRE_AND_PURGE;
        }else {
            //注册下一个用于提前触发的计时器
            long t = ctx.getCurrentWatermark() + (1000 - ctx.getCurrentWatermark() % 1000);
            if (t < window.maxTimestamp()){
                ctx.registerEventTimeTimer(t);
            }
            //提前触发窗口计算发出早期结果,但是不清理窗口数据
            return TriggerResult.FIRE;
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        // 因为我们在这个Trigger中使用了firstSeen这个触发器状态，因此需要在这里清理
        ValueState<Boolean> firstSeen = ctx.getPartitionedState(firstSeenDesc);
        firstSeen.clear();
    }
}
