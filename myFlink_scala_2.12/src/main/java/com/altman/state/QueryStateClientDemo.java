package com.altman.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 创建一个可查询式客户端，并从一个正在运行的flink job中查询所需的状态值
 * @author : Altman
 */
public class QueryStateClientDemo {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        // 创建客户端，proxyPort为9096
        QueryableStateClient client = new QueryableStateClient("localhost",9069);
        // 状态描述符，这个要跟flink应用中的一样
        ValueStateDescriptor<Double> lastTempDesc = new ValueStateDescriptor<>("lastTemp", Double.class);
        // 从日志中找到的jobId，你找不到，可以在日志中点击生成的rest地址，然后点击里面运行的任务，看到这个jobId
        String jobId = "0a2a63fe442ddb7d090466d996a80b77";
        // queryStateName要跟flink应用中的一致,sensor_1是需要查询的状态对应的key值，如果没有查到，可以自己修改这个key值
        CompletableFuture<ValueState<Double>> kvState = client.getKvState(JobID.fromHexString(jobId),
                "queryStateTest", "sensor_1", TypeInformation.of(String.class), lastTempDesc);
        ValueState<Double> doubleValueState = kvState.get();
        Double value = doubleValueState.value();
        System.out.println("获取到了值：" + value);

    }
}
