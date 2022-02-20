package com.altman.state;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 当同一个传感器的前后温度差值绝对值超过1.7就发出警报
 * @author : Altman
 */
public class TempAlertFunctionDemo {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        // 开启可查询服务
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER,true);
        config.set(QueryableStateOptions.PROXY_PORT_RANGE,"9096");  //代理服务的端口号，默认值就是9096，这个端口就是客户端需要查询的
        config.set(QueryableStateOptions.SERVER_PORT_RANGE,"9097");  // 设置服务端端口号，默认值就是9097
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.addSource(new SensorSource())
                .keyBy(SensorReading::getId)
                .flatMap(new TempAlertFunction(1.7))
                .print();
        env.execute();
    }
}
