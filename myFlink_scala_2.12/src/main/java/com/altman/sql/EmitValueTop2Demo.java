package com.altman.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Created on 2022/1/15 11:11 <br>
 * Copyright (c) 2022/1/15,一个很有个性的名字 所有 <br>
 * 测试TableAggregateFunction中的emitValue()
 * @author : Altman
 */
public class EmitValueTop2Demo {

    public static class Top2Accumulator {
        /**
         * top 1
         */
        public Integer first = Integer.MIN_VALUE ;
        /**
         * top 2
         */
        public Integer second = Integer.MIN_VALUE ;
    }

    public static class EmitValueTop2 extends TableAggregateFunction<Tuple2<Integer,Integer>,Top2Accumulator>{

        @Override
        public Top2Accumulator createAccumulator() {
            return new Top2Accumulator();
        }

        public void accumulate(Top2Accumulator accumulator,Integer value){
            if (value > accumulator.first){
                accumulator.second = accumulator.first ;
                accumulator.first = value ;
            }else if (value > accumulator.second){
                accumulator.second = value ;
            }
        }
        public void merge(Top2Accumulator accumulator,Iterable<Top2Accumulator> accumulators){
            for (Top2Accumulator top2Accumulator : accumulators) {
                accumulate(accumulator, top2Accumulator.first);
                accumulate(accumulator, top2Accumulator.second);
            }
        }

        public void emitValue(Top2Accumulator accumulator, Collector<Tuple2<Integer,Integer>> out){
            //发出top1 , 这里需要保证在accumulate(...)执行之前调用此方法也能正确处理，所以需要判断是否为Integer.MIN_VALUE
            if (accumulator.first != Integer.MIN_VALUE){
                out.collect(Tuple2.of(1,accumulator.first));
            }
            //发出top2
            if (accumulator.second != Integer.MIN_VALUE){
                out.collect(Tuple2.of(2,accumulator.second));
            }
        }
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        streamTableEnv.executeSql("CREATE TEMPORARY VIEW view_1(`id`,`name`,`price`) AS " +
                "VALUES (1,'Latte',6),(2,'Milk',3),(3,'Breve',5),(4,'Mocha',8),(5,'Tea',4)");

        streamTableEnv.createFunction("top2Function",EmitValueTop2.class);

        // tableAggregateFunction仅支持在Table API下使用：
        streamTableEnv.from("view_1")
                .flatAggregate(Expressions.call("top2Function",$("price")).as("rank","value"))
                .select($("rank"),$("value")).execute().print();

        //打印结果如下；如下内容表示，每次emitValue()之前，需要先发出delete语句，删除原来的TOP-N的结果，然后插入新的
        /*
        +----+-------------+-------------+
        | op |        rank |       value |
        +----+-------------+-------------+
        | +I |           1 |           6 |
        | -D |           1 |           6 |
        | +I |           1 |           6 |
        | +I |           2 |           3 |
        | -D |           1 |           6 |
        | -D |           2 |           3 |
        | +I |           1 |           6 |
        | +I |           2 |           5 |
        | -D |           1 |           6 |
        | -D |           2 |           5 |
        | +I |           1 |           8 |
        | +I |           2 |           6 |
        | -D |           1 |           8 |
        | -D |           2 |           6 |
        | +I |           1 |           8 |
        | +I |           2 |           6 |
        +----+-------------+-------------+
         */
    }
}
