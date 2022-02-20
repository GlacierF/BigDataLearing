package com.altman.connectors.kafka;

import com.altman.util.SensorReading;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * 实现自定义的SerializationSchema，用于将{@link SensorReading}序列化为字节数组传递给kafka
 * @author : Altman
 */
public class MySerializationSchema implements SerializationSchema<SensorReading> {

    @Override
    public void open(InitializationContext context) throws Exception {
        // 用于执行准备工作，一个任务实例只会执行一次，这里我们并不需要
    }

    @Override
    public byte[] serialize(SensorReading element) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(element);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("序列化SensorReading对象失败！",e);
        }
    }
}
