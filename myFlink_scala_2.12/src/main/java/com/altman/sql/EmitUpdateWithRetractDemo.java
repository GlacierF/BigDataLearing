package com.altman.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.planner.codegen.agg.ImperativeAggCodeGen;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * Created on 2022/1/15 12:09 <br>
 * Copyright (c) 2022/1/15,一个很有个性的名字 所有 <br>
 * 测试emitValue和emitUpdateWithRetract方法的优先级，需要通过DEBUG才能判断到底调用了哪个方法
 * 另外，如果删除emitValue方法，应该不会报错才对，但实际上报错了，详见{@link ImperativeAggCodeGen#checkNeededMethods(boolean, boolean, boolean, boolean, boolean)}
 * 方法中的内容，你会看到没有检查emitUpdateWithRetract的地方。
 * @author : Altman
 */
public class EmitUpdateWithRetractDemo {

    public static class Top2WithRetractAccumulator{
        public Integer first = Integer.MIN_VALUE;
        public Integer second = Integer.MIN_VALUE;
        public Integer oldFirst = Integer.MIN_VALUE;
        public Integer oldSecond = Integer.MIN_VALUE;
    }

    public static class EmitUpdateWithRetractTop2 extends TableAggregateFunction<Tuple2<String,Integer>,Top2WithRetractAccumulator>{

        @Override
        public Top2WithRetractAccumulator createAccumulator() {
            return new Top2WithRetractAccumulator();
        }

        public void accumulate(Top2WithRetractAccumulator accumulator,Integer value){
            if (value > accumulator.first){
                accumulator.second = accumulator.first;
                accumulator.first = value ;
            }else if (value > accumulator.second){
                accumulator.second = value ;
            }
        }

        public void emitUpdateWithRetract(Top2WithRetractAccumulator accumulator, RetractableCollector<Tuple2<String,Integer>> out){
            if (!accumulator.first.equals(accumulator.oldFirst)){
                //说明是更新操作且已经发出过记录，撤回原来的记录，然后发出新记录
                if (accumulator.oldFirst != Integer.MIN_VALUE){
                    out.retract(Tuple2.of("top1",accumulator.oldFirst));
                }
                //发出新记录,并将发出的值保存到oldFirst中
                out.collect(Tuple2.of("top1",accumulator.first));
                accumulator.oldFirst = accumulator.first ;
            }
            if (!accumulator.second.equals(accumulator.oldSecond)){
                //说明是更新操作且已经发出过记录，撤回原来的记录，然后发出新记录
                if (accumulator.oldSecond != Integer.MIN_VALUE){
                    out.retract(Tuple2.of("top2",accumulator.oldSecond));
                }
                //发出新记录，并将发出的值保存到oldSecond中,用于后续的回撤
                out.collect(Tuple2.of("top2",accumulator.second));
                accumulator.oldSecond = accumulator.second;
            }
        }

        public void emitValue(Top2WithRetractAccumulator accumulator, Collector<Tuple2<String,Integer>> out){
            if (accumulator.first != Integer.MIN_VALUE){
                out.collect(Tuple2.of("top1",accumulator.first));
            }
            if (accumulator.second != Integer.MIN_VALUE){
                out.collect(Tuple2.of("top2",accumulator.second));
            }
        }
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);


        streamTableEnv.executeSql("CREATE TEMPORARY VIEW view_1(`id`,`name`,`price`) AS " +
                "VALUES (1,'Latte',6),(2,'Milk',3),(3,'Breve',5),(4,'Mocha',8),(5,'Tea',4)");

        streamTableEnv.createFunction("top2WithRetractFunction",EmitUpdateWithRetractTop2.class);

        streamTableEnv.from("view_1")
                .flatAggregate(call("top2WithRetractFunction",$("price")).as("rank","value"))
                .select($("rank"),$("value")).execute().print();

    }
}
