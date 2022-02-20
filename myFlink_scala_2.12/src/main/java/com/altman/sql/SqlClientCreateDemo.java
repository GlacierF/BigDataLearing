package com.altman.sql;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.Duration;
import java.util.Locale;

/**
 * Created on 2022/1/8 11:40 <br>
 * Copyright (c) 2022/1/8,一个很有个性的名字 所有 <br>
 * 使用自定义函数，并完成  CREATE 语句的基本功能
 * @author : Altman
 */
public class SqlClientCreateDemo {

    public static class ToUppercase extends ScalarFunction{
        public String eval(String origin){
            if (origin == null){
                return null;
            }else {
                return origin.toUpperCase(Locale.ENGLISH);
            }
        }
    }
    public static void main(String[] args) {
        // 创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                //.inBatchMode()
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        // 执行创表语句（用于注册表格到环境中，这里使用了jdbc connector）
        tableEnvironment.executeSql("CREATE TABLE `default_catalog`.`default_database`.`my_user` (\n" +
                "  `id` BIGINT,\n" +
                "  `name` VARCHAR(2147483647),\n" +
                "  `age` INT,\n" +
                "  `created_time` TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'password' = 'root',\n" +
                "  'table-name' = 'my_user',\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://192.168.202.1:3306/mydb',\n" +
                "  'username' = 'root'\n" +
                ")");
        // 执行创建函数的语句（注册函数到环境中）
        tableEnvironment.executeSql("CREATE FUNCTION toUppercase AS 'com.altman.sql.SqlClientCreateDemo$ToUppercase' LANGUAGE JAVA");
        // 执行查询语，并应用注册的函数
        tableEnvironment.sqlQuery("select id,toUppercase(name),age,created_time from my_user").execute().print();

    }
}
