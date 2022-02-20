package com.altman.sourcAndSink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * 自定义数据源函数，并实现容错保证，注意这里使用了同步锁
 * @author : Altman
 */
public class ResettableCountSource implements CheckpointedFunction, SourceFunction<Long> {

    private volatile boolean isRunning = true;

    private Long count = 0L;

    private ListState<Long> listState;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listState.clear();
        listState.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> desc = new ListStateDescriptor<>("count", Long.class);
        listState = context.getOperatorStateStore().getListState(desc);
        // 如果是任务重启，那么就从检查点中读取到原来保存的信息
        if (context.isRestored()) {
            Iterable<Long> it = listState.get();
            for (Long aLong : it) {
                count += aLong;
            }
        }
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning && count < Long.MAX_VALUE) {
            // 获取锁，防止snapshotState方法和下面的代码同时运行
            synchronized (ctx.getCheckpointLock()) {
                count++;
                long timestamp = System.currentTimeMillis();
                ctx.collectWithTimestamp(count, timestamp);
                //发出水位线
                ctx.emitWatermark(new Watermark(timestamp - 1000L));
                Thread.sleep(200);
            }
        }
    }


    @Override
    public void cancel() {
        isRunning = false;
    }
}