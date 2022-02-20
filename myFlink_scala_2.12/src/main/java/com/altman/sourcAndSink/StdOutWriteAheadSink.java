package com.altman.sourcAndSink;

import com.altman.util.SensorReading;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

/**
 * 实现{@link GenericWriteAheadSink}接口，并在检查点完成之后将记录打印到控制台
 * @author : Altman
 */
public class StdOutWriteAheadSink extends GenericWriteAheadSink<SensorReading> {

    public StdOutWriteAheadSink(CheckpointCommitter committer, TypeSerializer<SensorReading> serializer, String jobID) throws Exception {
        super(committer, serializer, jobID);
    }

    @Override
    protected boolean sendValues(Iterable<SensorReading> values, long checkpointId, long timestamp) throws Exception {
        //在检查点完成之后，将记录写出到外部系统，这里我们直接打印到控制台
        for (SensorReading value : values) {
            System.out.println(value);
        }
        //写出完成之后，返回true
        return true;
    }
}
