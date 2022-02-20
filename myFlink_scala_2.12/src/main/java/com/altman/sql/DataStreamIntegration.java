package com.altman.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created on 2022/1/9 10:58 <br>
 * Copyright (c) 2022/1/9,一个很有个性的名字 所有 <br>
 *
 * @author : Altman
 */
public class DataStreamIntegration {
    public static void main(String[] args) throws Exception {
        //创建DataStream的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建Table的运行环境，并且需要实现DataStream与Table的转换
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        //创建数据源
        DataStreamSource<String> source = env.fromElements("altman", "bonnie", "candy");
        //将一个insert-only的datastream转为一个table
        Table inputTable = streamTableEnv.fromDataStream(source);
        // 为table注册一个视图，然后查询这个视图
        streamTableEnv.createTemporaryView("inputTable",inputTable);
        //查询后得到的动态表
        Table resultTable = streamTableEnv.sqlQuery("SELECT UPPER(f0) from inputTable");
        // 将查询结果表转为DataStream
        DataStream<Row> resultStream = streamTableEnv.toDataStream(resultTable);

        //打印查询出的DataStream
        resultStream.print() ;
        env.execute() ;
        //输出内容如下：
//        15> +I[ALTMAN]
//        16> +I[BONNIE]
//        1> +I[CANDY]
    }
}
