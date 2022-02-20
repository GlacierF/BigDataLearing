package com.altman.connectors.kafka;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * 实现使用{@link KafkaSink}将传感器中的数据写入到kafka中，
 * 因为我们是虚拟机反应比较慢，所以我这里把请求超时时间设置的比较长，发送数据超时时间也长
 * 相应的checkpoint的周期也更加的长来适应这个变动。不然会经常看到报错，而导致事务实际上一致没有提交成功
 * @author : Altman
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        //设置生产者的事务超时时间为5分钟，因为默认kafka服务的最大事务超时时间是15分钟，而Flink的写kafka的默认事务超时时间1个小时
        // 这会因为Flink的事务时间大于kafka支持的最大事务时间而抛出异常，可以修改Flink的事务超时时间小于15分钟，或者修改kafka集群支持的最大事务时间
        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms","40000"); //40秒
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"1");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");  //等待写出的数据确认收到
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,"10000"); // 发送请求的超时时间为10秒
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,"20000"); // 发送数据的超时时间为20秒
        KafkaSink<SensorReading> sink = KafkaSink.<SensorReading>builder()
                .setBootstrapServers("nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<SensorReading>builder()
                                .setTopic("SensorReading")
                                .setPartitioner(new MyKafkaPartitioner())
                                .setValueSerializationSchema(new MySerializationSchema())
                                .build()

                )
                // 设置精确一次写到kafka
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 事务id前缀
                .setTransactionalIdPrefix("myTransactionId-003")
                .setKafkaProducerConfig(properties)
                .build();
        //获取数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint 周期
        env.enableCheckpointing(30000);
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );
        source.sinkTo(sink);
        env.execute("SensorReadingSinkToKafka");
    }
}
