package com.altman.window;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 *  测试基于时间间隔的双流join，注意使用fromElement()方法获取数据源时，这个是一个有界的数据流，
 * @author : Altman
 */
public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        long currentTime = System.currentTimeMillis();
        System.out.println(currentTime);
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> source1 = env.fromElements(
                Tuple3.of("a", currentTime, 1),
                Tuple3.of("b", currentTime + 1, 1),
                Tuple3.of("b",currentTime + 2,1),
                Tuple3.of("c", currentTime + 3, 1))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> element.f1))
                );

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> source2 = env.fromElements(
                Tuple3.of("a", currentTime, 1),
                Tuple3.of("b", currentTime, 1),
                Tuple3.of("c", currentTime + 4, 1))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> element.f1))
                );
        source1.keyBy(value -> value.f0)
                .intervalJoin(source2.keyBy(value -> value.f0))
                .between(Time.milliseconds(-2),Time.milliseconds(1))
                // 不包括上界 范围是[a.timestamp - 2,b.timestamp + 1)
                .upperBoundExclusive()
                .process(new ProcessJoinFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, Tuple3<String,Long,Integer>>() {
                    @Override
                    public void processElement(Tuple3<String, Long, Integer> left, Tuple3<String, Long, Integer> right,
                                               Context ctx, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        out.collect(Tuple3.of(left.f0 + "==" + right.f0, ctx.getTimestamp(), left.f2 + right.f2));
                    }
                }).print();
        env.execute();
    }
}
