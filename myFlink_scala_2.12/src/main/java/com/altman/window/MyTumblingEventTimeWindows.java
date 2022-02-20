package com.altman.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * 自定义的滚动窗口分配器，窗口大小用毫秒数表示，
 * 此示例代码参考了{@link TumblingEventTimeWindows}
 * @author : Altman
 */
public class MyTumblingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {
    /**
     * 窗口大小,用毫秒表示的窗口大小
     */
    private long windowSize ;

    public MyTumblingEventTimeWindows(long windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        // 计算窗口开始时间，参考窗口TimeWindow的计算方式
        long startTime = timestamp - timestamp % windowSize;
        long endTime = startTime + windowSize;
        return Collections.singletonList(new TimeWindow(startTime,endTime));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        // 创建一个触发器，这个触发器是Flink内置的事件时间触发器
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
