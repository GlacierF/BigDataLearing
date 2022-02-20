package com.altman.datastream;

import com.altman.util.SensorReading;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 如果出现温度低于32华氏度，那么就像副输出发出冻结警告
 * @author : Altman
 */
public class FreezingMonitor extends ProcessFunction<SensorReading,SensorReading> {

    private final OutputTag<String> freezingAlarmOutput = new OutputTag<String>("freezing-alarms"){};
    @Override
    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
        if (value.getTemperature() < 32){
            //如果温度小于32华氏度，我们就发出冻结警告
            ctx.output(freezingAlarmOutput,String.format("Freezing Alarm for %s",value.getId()));
        }
        //将所有读数正常发出
        out.collect(value);
    }
}
