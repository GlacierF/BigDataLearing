package com.altman.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Created on 2022/1/15 9:29 <br>
 * Copyright (c) 2022/1/15,一个很有个性的名字 所有 <br>
 * 测试自定义AggregateFunction，本示例用于计算每个人的平均体重
 * @author : Altman
 */
public class WeightAverageFunctionDemo {
    /**
     * 用于统计累计体重及数量的累加器
     */
    public static class WeightAccumulator {
        /**
         * 累计体重
         */
        public Integer sum = 0 ;
        /**
         * 数量
         */
        public Integer count = 0 ;

    }

    public static class WeightAverageFunction extends AggregateFunction<Integer,WeightAccumulator>{
        /**
         * 计算平均值，并返回
         * @param accumulator 中间结果的累加器
         * @return 返回计算出的平均值
         */
        @Override
        public Integer getValue(WeightAccumulator accumulator) {
            if (accumulator.count == 0){
                return null;
            }
            return accumulator.sum / accumulator.count;
        }

        /**
         * 创建一个空的累加器，这个累加器将会被保存到状态中
         */
        @Override
        public WeightAccumulator createAccumulator() {
            return new WeightAccumulator();
        }

        /**
         * 实现累加逻辑
         * @param accumulator 保存中间结果的累加器
         * @param weight 需要加入的体重
         */
        public void accumulate(WeightAccumulator accumulator,Integer weight){
            accumulator.sum += weight;
            accumulator.count += 1;
        }

        /**
         * 将一组累加器合并为一个累加器
         * @param accumulator 当前累加器的值
         * @param accumulators 一组需要合并的累加器
         */
        public void merge(WeightAccumulator accumulator,Iterable<WeightAccumulator> accumulators){
            for (WeightAccumulator weightAccumulator : accumulators) {
                accumulator.sum += weightAccumulator.sum;
                accumulator.count += weightAccumulator.count;
            }
        }

        /**
         * 回撤一条记录，因此需要从累加器中删除这个记录的影响
         * @param accumulator 累加器
         * @param weight 需要回撤的值
         */
        public void retract(WeightAccumulator accumulator,Integer weight){
            accumulator.sum -= weight;
            accumulator.count -= 1 ;
        }

    }

    public static void main(String[] args) {
        //获取表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // 注册临时视图，并指定字段和数据
        streamTableEnv.executeSql("CREATE TEMPORARY VIEW view_1(`id`,`name`,`weight`) AS " +
                "VALUES (1,'altman',55),(2,'bonnie',66),(3,'candy',44)," +
                "(4,'altman',66),(5,'bonnie',44)");
        //注册函数
        streamTableEnv.createFunction("weightAvg",WeightAverageFunction.class);
        // 执行聚合查询
        streamTableEnv.executeSql("SELECT `name`,weightAvg(`weight`) FROM view_1 GROUP BY `name`").print();
        //打印结果如下：
        /*
        +----+----------+-------------+
        | op |     name |      EXPR$1 |
        +----+----------+-------------+
        | +I |    candy |          44 |
        | +I |   altman |          55 |
        | +I |   bonnie |          66 |
        | -U |   altman |          55 |
        | +U |   altman |          60 |
        | -U |   bonnie |          66 |
        | +U |   bonnie |          55 |
        +----+----------+-------------+
         */
    }
}
