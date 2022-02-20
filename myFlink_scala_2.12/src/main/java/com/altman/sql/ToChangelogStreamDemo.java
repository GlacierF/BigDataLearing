package com.altman.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created on 2022/1/9 11:29 <br>
 * Copyright (c) 2022/1/9,一个很有个性的名字 所有 <br>
 * 代码示例含有update操作的Table如何转为一个DataStream，以及Stream模式跟batch模式下的两个结果的对比
 * @author : Altman
 */
public class ToChangelogStreamDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //流式模式
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //也可以改用批模式
         env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        //获取输入流
        DataStreamSource<Row> source = env.fromElements(
                Row.of("altman", 23),
                Row.of("bonnie", 24),
                Row.of("altman", 100)
        );
        //将一个insert-only的DataStream转为table，并指定字段名为name,score
        Table inputTable = streamTableEnv.fromDataStream(source).as("name", "score");
        //为表格注册一个视图，并且查询
        streamTableEnv.createTemporaryView("inputTable",inputTable);
        Table resultTable = streamTableEnv.sqlQuery("SELECT name,SUM(score) from inputTable group by name");

        //将含有更新操作的table转为一个changelog DataStream
        DataStream<org.apache.flink.types.Row> resultDataStream = streamTableEnv.toChangelogStream(resultTable);

        //打印流中的数据
        resultDataStream.print();
        env.execute();
        // stream模式 打印结果如下
//        2> +I[bonnie, 24]
//        12> +I[altman, 23]
//        12> -U[altman, 23]
//        12> +U[altman, 123]

        // batch模式，打印结果如下！batch模式下，inputTable在发出结果之前，会自己消费数据，并将结果转为一个insert-only的表格
//        6> +I[bonnie, 24]
//        14> +I[altman, 123]
    }
}
