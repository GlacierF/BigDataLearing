package com.altman.window;

import com.altman.util.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 计算每个传感器记录的平均温度
 * @author : Altman
 */
public class AvgTempFunction implements AggregateFunction<SensorReading, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {

    /**
     * 定义一个中间结果累加器
     * @return 返回一个三元素元组，第一个值保存传感器id，第二个值保存累加的温度，第三个值保存累加的次数
     */
    @Override
    public Tuple3<String, Double, Integer> createAccumulator() {
        return Tuple3.of("",0.0,0);
    }

    /**
     * 将输入的传感器数据累加到中间结果
     * @param value 输入的传感器数据
     * @param accumulator 中间结果累加器
     * @return 返回累加后的新的中间结果
     */
    @Override
    public Tuple3<String, Double, Integer> add(SensorReading value, Tuple3<String, Double, Integer> accumulator) {
        return Tuple3.of(value.getId(),accumulator.f1 + value.getTemperature(),accumulator.f2 + 1);
    }

    /**
     * 根据累加器的值，计算出平均温度
     * @param accumulator 中间结果累加器
     * @return 返回一个元组，第一个元素是传感器id，第二个元素是平均值
     */
    @Override
    public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
        return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
    }

    /**
     * 将两个累加器合并
     * @param a 累加器a
     * @param b 累加器b
     * @return 返回合并后的新的累加器
     */
    @Override
    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
        return Tuple3.of(a.f0,a.f1 + b.f1,a.f2 + b.f2);
    }
}
