package com.altman.state;

import com.altman.util.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 此flatMap用于检测同个传感器相邻的两个温度的差值是否超过了阈值
 * 因为需要使用状态，所以要用RichFunction用来访问RuntimeContext <br>
 * 此示例还将状态设置为了可查询式的了
 * @author : Altman
 */
public class TempAlertFunction extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>> {

    /**
     * 阈值
     */
    private final Double threshold;
    /**
     * 状态应用对象
     */
    private transient ValueState<Double> lastTempState;

    public TempAlertFunction(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //通常我们要在open方法中初始化状态引用对象
        ValueStateDescriptor<Double> lastTempDesc = new ValueStateDescriptor<>("lastTemp", Double.class);
        // 将状态设置为可查询的状态
        lastTempDesc.setQueryable("queryStateTest");
        lastTempState = getRuntimeContext().getState(lastTempDesc);
    }

    @Override
    public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
        //从状态中获取此传感器上一次的温度
        Double lastTemp = this.lastTempState.value();
        // 检查是否需要发出警报
        if (lastTemp != null){
            //得到两次温度的差值绝对值
            double tempDiff = Math.abs(value.getTemperature() - lastTemp);
            if (tempDiff > threshold){
                //超过阈值，向下游发出警报
                out.collect(Tuple3.of(value.getId(), value.getTemperature(), tempDiff));
            }
        }
        //更新温度状态
        lastTempState.update(value.getTemperature());
    }
}
