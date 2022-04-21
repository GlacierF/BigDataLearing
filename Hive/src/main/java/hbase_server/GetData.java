package hbase_server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import hbase_server.HbaseUtil;
public class GetData {
    public static void main(String[] args) throws Exception {


      String[] dt =   HbaseUtil.getResult( TableName.valueOf("badou_orders"),"1");
        System.out.println(dt.toString());
    }

}
