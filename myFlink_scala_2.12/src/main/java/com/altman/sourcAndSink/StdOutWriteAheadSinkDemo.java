package com.altman.sourcAndSink;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;

/**
 * 使用GenericWriteAheadSink接口 实现数据的写出操作，因为我这个电脑是16线程的
 * 因此有16个并行任务，你能看到src/mian/file-checkpoint目录下有16个文件(0-15)，每个文件里面
 * 都保存了对应并行实例记录的检查点信息（实际上就是检查点编号，用于表示小于等于这个检查点编号的对应的记录已经全部成功写出到外部系统了）
 * @author : Altman
 */
public class StdOutWriteAheadSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5 * 1000);
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );
        // checkpointCommitter，检查点信息保存到本地
        FileCheckpointCommitter committer = new FileCheckpointCommitter("src/main/file-checkpoint");
        // 序列化器
        TypeSerializer<SensorReading> serializer = TypeInformation.of(SensorReading.class).createSerializer(new ExecutionConfig());
        //随机一个jobId即可
        String jobId = UUID.randomUUID().toString();
        StdOutWriteAheadSink writeAheadSink = new StdOutWriteAheadSink(committer,serializer,jobId);
        source.transform("writeAheadSink", TypeInformation.of(SensorReading.class), writeAheadSink);

        env.execute();
    }
}
