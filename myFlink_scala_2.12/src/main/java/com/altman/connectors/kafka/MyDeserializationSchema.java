package com.altman.connectors.kafka;

import com.altman.util.SensorReading;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * 自定义反序列化器，实现将kafka中的记录转为{@link SensorReading}对象
 * @author : Altman
 */
public class MyDeserializationSchema implements DeserializationSchema<SensorReading> {
    @Override
    public SensorReading deserialize(byte[] message) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(message);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        try {
            return (SensorReading) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new FlinkRuntimeException("无法将kafka中的记录转为SensorReading !" ,e);
        }
    }

    @Override
    public boolean isEndOfStream(SensorReading nextElement) {
        return false;
    }

    @Override
    public TypeInformation<SensorReading> getProducedType() {
        return TypeInformation.of(SensorReading.class);
    }
}
