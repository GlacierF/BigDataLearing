package com.altman.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Created on 2022/1/8 18:11 <br>
 * Copyright (c) 2022/1/8,一个很有个性的名字 所有 <br>
 *
 * @author : Altman
 */
public class DropDemo {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        tableEnvironment.executeSql("CREATE TABLE my_user(" +
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

        //打印当前表格列表
        tableEnvironment.executeSql("SHOW TABLES").print();
        //删除表格
        tableEnvironment.executeSql("DROP TABLE my_user");
        //打印当前表格
        tableEnvironment.executeSql("SHOW TABLES").print();
    }
}
