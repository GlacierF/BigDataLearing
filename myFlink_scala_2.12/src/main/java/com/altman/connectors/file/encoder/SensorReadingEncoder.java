package com.altman.connectors.file.encoder;

import com.alibaba.fastjson.JSON;
import com.altman.util.SensorReading;
import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * 使用{@link JSON}将数据序列化到文件中
 * @author : Altman
 */
public class SensorReadingEncoder implements Encoder<SensorReading> {

    private final String charsetName;

    private transient Charset charset;

    public SensorReadingEncoder(String charsetName) {
        this.charsetName = charsetName;
    }

    public SensorReadingEncoder() {
        this.charsetName = "UTF-8" ;
    }

    @Override
    public void encode(SensorReading element, OutputStream stream) throws IOException {
        if (charset == null){
            charset = Charset.forName(charsetName);
        }
        String str = JSON.toJSONString(element);
        stream.write(str.getBytes(charset));
        // 一条记录一行，所以这里需要写出一个换行符
        stream.write('\n');
    }
}
