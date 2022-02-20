package com.altman.sourcAndSink;


import com.alibaba.fastjson.JSON;
import com.altman.util.SensorReading;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Optional;

/**
 * 实现模拟事务写本地文件，基本过程如下：
 * <ol>
 *     <li>开启一个事务，我们就创建一个临时文件，文件名格式是： .part-taskId-fileId.inProgress</li>
 *     <li>预提交事务，将关闭临时文件输入流，并将临时文件路径保存到检查点中</li>
 *     <li>提交事务，将临时文件重命名为最终文件</li>
 * </ol>
 * 被示例中，使用了自定义的上下文环境来保存fileId，以及生成最终文件路径和临时文件路径
 *
 * @author : Altman
 */
public class TransactionalFileSink extends TwoPhaseCommitSinkFunction<SensorReading, Tuple2<String, String>, TransactionalFileSink.FileTransactionContext> {
    /**
     * 写数据到本地的writer
     */
    private BufferedWriter bufferedWriter;
    /**
     * 文件输出的根路径，所有文件都将在这个目录下
     */
    private final String basePath;

    public TransactionalFileSink(TypeSerializer<Tuple2<String, String>> transactionSerializer, TypeSerializer<FileTransactionContext> contextSerializer, String basePath) {
        super(transactionSerializer, contextSerializer);
        this.basePath = basePath;
    }

    @Override
    protected Optional<FileTransactionContext> initializeUserContext() {
        return Optional.of(new FileTransactionContext(String.valueOf(getRuntimeContext().getIndexOfThisSubtask()), basePath));
    }

    @Override
    protected void finishRecoveringContext(Collection<Tuple2<String, String>> handledTransactions) {
        //删除脏文件,所有id大于当前userContext记录的fileId的，都是脏文件
        long lastFileId = userContext.get().getFileId();
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        File file = new File(userContext.get().getBasePath());
        File[] files = file.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                //只保留taskId是当前任务的且fileId大于userContext最后记录的文件
                String name = pathname.getName();
                if (name.contains(".inprogress")) {
                    String substring = name.substring(0, name.indexOf(".inprogress"));
                    String[] split = substring.split("-");
                    int taskId = Integer.parseInt(split[1]);
                    long fileId = Long.parseLong(split[2]);
                    if (indexOfThisSubtask != taskId) {
                        return false;
                    } else {
                        return fileId > lastFileId;
                    }
                } else {
                    return false;
                }
            }
        });
        //将过滤出来的文件删除
        if (files != null && files.length != 0) {
            for (File f : files) {
                f.delete();
            }
        }
    }

    @Override
    protected void invoke(Tuple2<String, String> transaction, SensorReading value, Context context) throws Exception {
        //将记录写到临时文件中
        bufferedWriter.write(JSON.toJSONString(value));
        bufferedWriter.write('\n');
    }

    @Override
    protected Tuple2<String, String> beginTransaction() throws Exception {
        //开启一个新的事务
        if (userContext.isPresent()) {
            String nextTempFilePath = userContext.get().getNextTempFilePath();
            Path path = Paths.get(nextTempFilePath);
            if (Files.exists(path)) {
                //讲道理文件不可能存在，因为我们在恢复任务的时候，自动删除了脏文件
                throw new Exception("文件已经存在，请检查代码逻辑！");
            }
            bufferedWriter = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            //必须在开启事务的时候，将currentFilePath一起拿出来，因为重命名文件之前会重新开启一个事务，导致currentFilePath被修改了
            // 从而引起fileId为0的文件永远没有的BUG
            return Tuple2.of(nextTempFilePath, userContext.get().getCurrentFilePath());
        } else {
            throw new RuntimeException("FileTransactionContext未初始化!");
        }
    }

    @Override
    protected void preCommit(Tuple2<String, String> transaction) throws Exception {
        //将数据刷写到文件中，并关闭当前事务
        bufferedWriter.flush();
        bufferedWriter.close();
    }

    @Override
    protected void commit(Tuple2<String, String> transaction) {
        //检查文件是否存在，以保证提交的幂等性
        Path path = Paths.get(transaction.f0);
        if (Files.exists(path)) {
            Path currentFilePath = Paths.get(transaction.f1);
            try {
                Files.move(path, currentFilePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void abort(Tuple2<String,String> transaction) {
        Path path = Paths.get(transaction.f0);
        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * 用于自动生成下一个事务需要输出的文件路径，以及提交事务时获取目标文件路径
     * 此上下文环境维护了一个fileId属性，用于构造不同的文件名
     */
    public static class FileTransactionContext {
        /**
         * 当前任务的id
         */
        private final String taskId;
        /**
         * 文件名前缀，默认值为part
         */
        private String prefix = "part";
        /**
         * 当前任务输出的文件的编号，从0开始，表示输出的第几个文件
         */
        private long fileId = 0;
        /**
         * 当前临时文件对应的最终文件路径，
         * 路径格式形如：part-taskId-fileId
         */
        private String currentFilePath;
        /**
         * 文件输出的根路径
         */
        private final String basePath;

        /**
         * 使用<pre>{@code
         *    getRuntimeContext().getIndexOfThisSubtask()方法获取当前任务的id
         * }</pre>
         *
         * @param taskId   当前任务id
         * @param basePath 文件输出的根路径
         */
        public FileTransactionContext(String taskId, String basePath) {
            this.taskId = taskId;
            this.basePath = basePath;
            try {
                Files.createDirectories(Paths.get(basePath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public FileTransactionContext(String taskId, String prefix, String basePath) {
            this.taskId = taskId;
            this.prefix = prefix;
            this.basePath = basePath;
        }

        /**
         * 开启一个新的事务需要调用的方法，此方法返回一个临时文件路径，表示任务接下来要输出的文件临时路径
         *
         * @return 返回一个临时文件路径，形如prefix-taskId-fileId.inprogress
         */
        public String getNextTempFilePath() {
            currentFilePath = String.format("%s/%s-%s-%s", basePath, prefix, taskId, fileId);
            fileId++;
            return currentFilePath + ".inprogress";
        }

        public long getFileId() {
            return fileId;
        }

        public void setFileId(long fileId) {
            this.fileId = fileId;
        }

        public String getCurrentFilePath() {
            return currentFilePath;
        }

        public void setCurrentFilePath(String currentFilePath) {
            this.currentFilePath = currentFilePath;
        }

        public String getBasePath() {
            return basePath;
        }
    }
}
