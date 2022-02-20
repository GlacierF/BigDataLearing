package com.altman.async;


import com.google.common.util.concurrent.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.context.ExecutionContextImpl;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Created on 2021/12/26 15:41
 * Copyright (c) 2021/12/26,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class MysqlAsyncFunction extends RichAsyncFunction<Long, Tuple3<Long, String, Integer>> {

    private transient Connection connection;

    private transient ListeningExecutorService listeningExecutorService ;

    @Override
    public void open(Configuration parameters) throws Exception {

        //建立连接
        Class.forName("com.mysql.jdbc.Driver");
        // 获取连接，注意url可能需要自己改
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb","root","root");

        listeningExecutorService =  MoreExecutors.listeningDecorator(Executors.newCachedThreadPool()) ;
    }

    @Override
    public void asyncInvoke(Long input, ResultFuture<Tuple3<Long, String, Integer>> resultFuture) throws Exception {
        ListenableFuture<Tuple3<Long, String, Integer>> listenableFuture = listeningExecutorService.submit(new Callable<Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> call() throws Exception {
                PreparedStatement preparedStatement = connection.prepareStatement(String.format("select id,name,age from person where id = '%s'", input));
                ResultSet resultSet = preparedStatement.executeQuery();
                // id对应的只有一行记录，所以不用循环遍历
                if (resultSet.next()) {
                    long id = resultSet.getLong(1);
                    String name = resultSet.getString(2);
                    int age = resultSet.getInt(3);
                    return Tuple3.of(id, name, age);
                }else {
                    //返回默认值,或者我们可以抛出异常
                    return Tuple3.of(0L,"default",0);
                }
            }
        });
        Futures.addCallback(listenableFuture, new FutureCallback<Tuple3<Long, String, Integer>>() {
            @Override
            public void onSuccess(Tuple3<Long, String, Integer> result) {
                resultFuture.complete(Collections.singleton(result));
            }

            @Override
            public void onFailure(Throwable t) {
                resultFuture.completeExceptionally(t);
            }
        });

        // 或者可以使用如下的方式
//        CompletableFuture<Tuple3<Long, String, Integer>> completableFuture = CompletableFuture.supplyAsync(() -> {
//            try {
//                PreparedStatement preparedStatement = connection.prepareStatement(String.format("select id,name,age from person where id = '%s'", input));
//                ResultSet resultSet = preparedStatement.executeQuery();
//                // id对应的只有一行记录，所以不用循环遍历
//                if (resultSet.next()) {
//                    long id = resultSet.getLong(1);
//                    String name = resultSet.getString(2);
//                    int age = resultSet.getInt(3);
//                    return Tuple3.of(id, name, age);
//                } else {
//                    //返回默认值  或者我们可以抛出异常！
//                    return Tuple3.of(0L, "default", 0);
//                }
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        },listeningExecutorService);
//        completableFuture.thenAccept(tupleResult -> {
//            resultFuture.complete(Collections.singleton(tupleResult));
//        });
//        completableFuture.exceptionally(throwable -> {
//            resultFuture.completeExceptionally(throwable);
//            return null;
//        });

    }

    @Override
    public void close() throws Exception {
        listeningExecutorService.shutdown();
        connection.close();
    }
}
