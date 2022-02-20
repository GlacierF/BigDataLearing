package com.altman.async;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple3;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created on 2021/12/26 17:27
 * Copyright (c) 2021/12/26,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class CompletableFutureDemo {

    public static void main(String[] args) throws InterruptedException, ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
//         创建CompletableFuture时传入Supplier对象
//        CompletableFuture<Integer> future = CompletableFuture.supplyAsync(new MySupplier());
//        //执行成功时
//        future.thenAccept(new MyConsumer());
//        // 执行异常时
//        future.exceptionally(new MyFunction());
//        // 主任务可以继续处理，不用等任务执行完毕
//        System.out.println("主线程继续执行");
//        Thread.sleep(5000);
//        System.out.println("主线程执行结束");
        System.out.println("主程序开始");
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb","root","root");

        CompletableFuture<List<Tuple3<Long, String, Integer>>> completableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                PreparedStatement preparedStatement = connection.prepareStatement("select id,name,age from person");
                ResultSet resultSet = preparedStatement.executeQuery();
                List<Tuple3<Long, String, Integer>> lists = new ArrayList<>();
                while (resultSet.next()) {
                    long id = resultSet.getLong(1);
                    String name = resultSet.getString(2);
                    int age = resultSet.getInt(3);
                    lists.add(Tuple3.of(id, name, age));
                }
                Thread.sleep(5000);
//                return lists;
                throw new RuntimeException("eeeeeee");
            } catch (SQLException | InterruptedException throwables) {
                throw new RuntimeException(throwables);
            }
        }, Executors.newCachedThreadPool());

        completableFuture.thenAccept(new Consumer<List<Tuple3<Long, String, Integer>>>() {
            @Override
            public void accept(List<Tuple3<Long, String, Integer>> tuple3s) {
                for (Tuple3<Long, String, Integer> tuple3 : tuple3s) {
                    System.out.println(tuple3);
                }
                throw new RuntimeException("ddddd");
            }
        });
        completableFuture.exceptionally(new Function<Throwable, List<Tuple3<Long, String, Integer>>>() {
            @Override
            public List<Tuple3<Long, String, Integer>> apply(Throwable throwable) {
                System.out.println("执行异常" + throwable);
                return Lists.newArrayList(Tuple3.of(0L,"default",1));
            }
        });

        System.out.println("主程序执行结束");



    }
}

class MySupplier implements Supplier<Integer> {
    @Override
    public Integer get() {
        try {
            // 任务睡眠3s
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 3 + 2;
    }
}

// 任务执行完成时回调Consumer对象
class MyConsumer implements Consumer<Integer> {
    @Override
    public void accept(Integer integer) {
        System.out.println("执行结果" + integer);
    }
}

// 任务执行异常时回调Function对象
class MyFunction implements Function<Throwable, Integer> {
    @Override
    public Integer apply(Throwable type) {
        System.out.println("执行异常" + type);
        return 0;
    }

}
