package com.altman.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Created on 2022/1/8 19:33 <br>
 * Copyright (c) 2022/1/8,一个很有个性的名字 所有 <br>
 * 测试insert into ... select语句
 * @author : Altman
 */
public class InsertDemo {
    public static void main(String[] args) {
        // 创建环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // 注册my_user表
        tableEnv.executeSql("CREATE TABLE my_user(" +
                "id bigint primary key not enforced, \n" +
                "name varchar,\n" +
                "age int,\n" +
                "created_time timestamp(3)\n" +
                ") with(\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://localhost:3306/mydb',\n" +
                " 'table-name' = 'my_user',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'root'\n" +
                ")");
        // 使用LIKE语句 注册my_usercp表
        tableEnv.executeSql("CREATE TABLE my_usercp \n" +
                "with(\n" +
                " 'table-name' = 'my_usercp' \n" +
                ") like my_user");
        // 将my_user表的内容写入到my_usercp
        tableEnv.executeSql("INSERT INTO my_usercp SELECT id,name,age,created_time from my_user");
    }
}
