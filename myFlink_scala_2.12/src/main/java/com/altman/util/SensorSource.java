package com.altman.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Random;
import java.util.stream.Stream;

/**
 * 每200ms为为每个传感器生成一个读取，共十个传感器（通过id判断），并传递到下游
 * @author : Altman
 */
public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private boolean isRunning = true;


    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        //构造一个随机数生成器
        final Random random = new Random();
        // 获取当前 并行实例（任务）的index值
        final int indexOfThisSubtask = this.getRuntimeContext().getIndexOfThisSubtask();
        // 构造一个List，每个元素都是一个传感器读数，元素是一个元组，元组的第一个值是传感器编号，第二个值是传感器读数
        final ArrayList<Tuple2<String, Double>> curTemp = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            curTemp.add(Tuple2.of("sensor_" + (indexOfThisSubtask * 10 + i),65 + (random.nextGaussian() * 20)));
        }
        while (isRunning){
            // 更新传感器读取到的温度
            Stream<Tuple2<String, Double>> mappedTemp = curTemp.stream().map(value -> Tuple2.of(value.f0, value.f1 + (random.nextGaussian() * 0.5)));
            //获取当前时间
            long timeInMillis = Calendar.getInstance().getTimeInMillis();
            //发送一个新的传感器读书
            mappedTemp.forEach(t -> ctx.collect(new SensorReading(t.f0,timeInMillis,t.f1)));
            //休息200ms
            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
