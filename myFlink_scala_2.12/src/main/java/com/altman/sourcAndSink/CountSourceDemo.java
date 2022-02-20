package com.altman.sourcAndSink;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 使用自定义的数据源，因为数据源中已经发出了水位线，所以入口类中不用再指定水位线策略了
 * @author : Altman
 */
public class CountSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setMaxConcurrentCheckpoints(3);
        checkpointConfig.setCheckpointTimeout(5000);
        checkpointConfig.setTolerableCheckpointFailureNumber(1);
        env.enableCheckpointing(1000);

        env.addSource(new ResettableCountSource())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
                //计算每个窗口内记录之和
                .reduce(Long::sum)
                .print();
        env.execute();
    }
}
