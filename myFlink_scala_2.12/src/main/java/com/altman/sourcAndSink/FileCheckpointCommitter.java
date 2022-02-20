package com.altman.sourcAndSink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 使用本地文件保存已经提交过的检查点信息
 * @author : Altman
 * @apiNote 这里的检查点信息是指检查点编号,表示这个小于等于这个检查点编号的那些检查点对应的记录信息已经成功写出到外部系统了，这个编号将由此Committer维护
 */
public class FileCheckpointCommitter extends CheckpointCommitter {
    /**
     * 检查点信息写出的路径
     */
    private String tmpPath;

    private final String basePath ;

    public FileCheckpointCommitter(String basePath) {
        this.basePath = basePath;
    }

    @Override
    public void open() throws Exception {
        //此方法通常用于来建立跟外部系统的连接，如连接数据库，hdfs、zookeeper,kafka
        //但示例是写到本地，不用建立连接
    }

    @Override
    public void close() throws Exception {
        //关闭连接
    }

    @Override
    public void createResource() throws Exception {
        tmpPath = basePath + "/"  + jobId;
        //创建本任务，检查点信息写出的路径
        Files.createDirectory(Paths.get(tmpPath));
    }

    @Override
    public void commitCheckpoint(int subtaskIdx, long checkpointID) throws Exception {
        //检查点对应的记录已经成功写出到外部系统了，此时将这个检查点编号记录到文件中
        Path committerPath = Paths.get(tmpPath + "/" + subtaskIdx);
        //将checkpoint转为字符串
        String hexId = "0x" + StringUtils.leftPad(Long.toHexString(checkpointID), 16, "0");
        //按utf-8字符集写到文件中
        Files.write(committerPath, hexId.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean isCheckpointCommitted(int subtaskIdx, long checkpointID) throws Exception {
        // 检查此checkpointId对应的记录是否写出过了

        // 先读回最后一次在文件中记录的checkpointId
        Path committerPath = Paths.get(tmpPath + "/" + subtaskIdx);
        if (!Files.exists(committerPath)) {
            //文件不存在，说明此任务还没有成功提交过记录到外部系统
            return false;
        } else {
            //将文件中的每一行记录都读出来，实际上文件只有一行信息
            String hexId = Files.readAllLines(committerPath).get(0);
            Long lastCheckpointId = Long.decode(hexId);
            //如果当前的checkpointId 小于等于 最后一次写到文件中记录的id，那么说明这个检查点已经提交过了
            return checkpointID <= lastCheckpointId;
        }
    }
}
