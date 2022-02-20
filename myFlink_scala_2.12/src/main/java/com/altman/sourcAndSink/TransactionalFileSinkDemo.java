package com.altman.sourcAndSink;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

/**
 * 实现自定义的{@link TwoPhaseCommitSinkFunction} 示例
 * @author : Altman
 */
public class TransactionalFileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5 * 1000);
        // 获取数据源 及 定义水位线策略
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );
        // 创建serializer对象
        TypeSerializer<Tuple2<String,String>> transactionSerializer = TypeInformation.of(new TypeHint<Tuple2<String,String>>() {}).createSerializer(new ExecutionConfig());
        TypeSerializer<TransactionalFileSink.FileTransactionContext> contextSerializer = TypeInformation.of(
                TransactionalFileSink.FileTransactionContext.class).createSerializer(new ExecutionConfig());
        //创建sink端
        TransactionalFileSink transactionalFileSink = new TransactionalFileSink(transactionSerializer, contextSerializer,"src/main/file-sink");
        // 将source数据输出到sink
        source.addSink(transactionalFileSink);

        env.execute();
    }
}
