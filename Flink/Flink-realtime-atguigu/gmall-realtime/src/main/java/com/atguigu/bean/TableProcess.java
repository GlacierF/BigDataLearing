package com.atguigu.bean;

import lombok.Data;

@Data
public class TableProcess {
    //动态分流Sink常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    //来源表
    String SourceTable;
    //操作类型 insert,update,delete
    String OperateType;
    //输出类型 hbase kafka
    String SinkType;
    //输出表(主题)
    String SinkTable;
    //输出字段
    String SinkColumns;
    //主键字段
    String SinkPk;
    //建表扩展
    String SinkExtend;

}
