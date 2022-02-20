package com.altman.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Created on 2022/1/15 13:43 <br>
 * Copyright (c) 2022/1/15,一个很有个性的名字 所有 <br>
 *
 * @author : Altman
 */
public class Top2Demo {
    public static class Top2WithRetractAccumulator {
        public Integer first;
        public Integer second;
        public Integer oldFirst;
        public Integer oldSecond;
    }

    public static class Top2WithRetract
            extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2WithRetractAccumulator> {

        @Override
        public Top2WithRetractAccumulator createAccumulator() {
            Top2WithRetractAccumulator acc = new Top2WithRetractAccumulator();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            acc.oldFirst = Integer.MIN_VALUE;
            acc.oldSecond = Integer.MIN_VALUE;
            return acc;
        }

        public void accumulate(Top2WithRetractAccumulator acc, Integer v) {
            if (v > acc.first) {
                acc.second = acc.first;
                acc.first = v;
            } else if (v > acc.second) {
                acc.second = v;
            }
        }

        public void emitUpdateWithRetract(
                Top2WithRetractAccumulator acc,
                RetractableCollector<Tuple2<Integer, Integer>> out) {
            if (!acc.first.equals(acc.oldFirst)) {
                // if there is an update, retract the old value then emit a new value
                if (acc.oldFirst != Integer.MIN_VALUE) {
                    out.retract(Tuple2.of(acc.oldFirst, 1));
                }
                out.collect(Tuple2.of(acc.first, 1));
                acc.oldFirst = acc.first;
            }
            if (!acc.second.equals(acc.oldSecond)) {
                // if there is an update, retract the old value then emit a new value
                if (acc.oldSecond != Integer.MIN_VALUE) {
                    out.retract(Tuple2.of(acc.oldSecond, 2));
                }
                out.collect(Tuple2.of(acc.second, 2));
                acc.oldSecond = acc.second;
            }
        }
    }

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);


        streamTableEnv.executeSql("CREATE TEMPORARY VIEW view_1(`id`,`name`,`price`) AS " +
                "VALUES (1,'Latte',6),(2,'Milk',3),(3,'Breve',5),(4,'Mocha',8),(5,'Tea',4)");

        streamTableEnv.createFunction("top2WithRetractFunction",Top2WithRetract.class);

        streamTableEnv.from("view_1")
                .flatAggregate(call("top2WithRetractFunction",$("price")).as("rank","value"))
                .select($("rank"),$("value")).execute().print();

    }
}
