package com.altman.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

/**
 * Created on 2022/1/7 7:56 <br>
 * Copyright (c) 2022/1/7,一个很有个性的名字 所有 <br>
 *
 * @author : Altman
 */
public class CreateStatementDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
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
        ////////////////
        // create demo
        ////////////////
//        Table table = tableEnvironment.sqlQuery("select id,name,age,created_time from my_user");
//        TableResult result = table.execute();
//        result.print();

        //////////////////
        // drop demo
        /////////////////
//        tableEnvironment.executeSql("show tables").print();
//
//        tableEnvironment.executeSql("drop table my_user");
//
//        tableEnvironment.executeSql("show tables").print();

        //////////////////
        //insert demo
        /////////////////
        tableEnvironment.executeSql("CREATE TABLE my_usercp(" +
                "id bigint primary key not enforced, \n" +
                "name varchar,\n" +
                "age int,\n" +
                "created_time timestamp(3)\n" +
                ") with(\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://localhost:3306/mydb',\n" +
                " 'table-name' = 'my_usercp',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'root'\n" +
                ")");

        TableResult tableResult = tableEnvironment.executeSql("insert into my_usercp select id,name,age,created_time from my_user");



    }
}
