package hbase_server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseUtil {
    static Connection conn = null;

    static {

        //创建连接对象

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "192.168.77.10:2181,192.168.77.11:2181,192.168.77.12:2181");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Table getTable(String tableName) throws Exception {

        TableName tbName = TableName.valueOf(tableName);
        return conn.getTable(tbName);
    }
    public static Connection getConn() {
        return conn;
    }
    public static Admin getAdmin() throws Exception {
        return conn.getAdmin();
    }
    public static void updateData(TableName tableName, String rowKey, String columnFamily, String columnName, String columnValue) throws Exception{
        Table table = conn.getTable(tableName);
        Put put1 = new Put(Bytes.toBytes(rowKey));
        put1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
        table.put(put1);
        table.close();
    }
    public static String[] getResult(TableName tableName, String rowKey) throws Exception {
        Table table = conn.getTable(tableName);
        //获得一行
        Get get = new Get(Bytes.toBytes(rowKey));
        Result set = table.get(get);
        Cell[] cells = set.rawCells();
        String [] data = new String[2];
        int i=0;
        for (Cell cell : cells) {
            System.out.println( Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            data[i]=Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            i+=1;
        }
        table.close();
        return data;
    }
    public static void deleteData(TableName tableName, String rowKey,  String columnFamily, String columnName) throws Exception{
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //①根据rowKey删除一行数据
        table.delete(delete);

        //②删除某一行的某一个列簇内容
        delete.addFamily(Bytes.toBytes(columnFamily));

        //③删除某一行某个列簇某列的值
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        table.close();
    }
    public static void insertMany() throws Exception{
        Table table = conn.getTable(TableName.valueOf("test"));
        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("rowKey1"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("wd"));

        Put put2 = new Put(Bytes.toBytes("rowKey2"));
        put2.addColumn(Bytes.toBytes("user"), Bytes.toBytes("age"), Bytes.toBytes("25"));

        Put put3 = new Put(Bytes.toBytes("rowKey3"));
        put3.addColumn(Bytes.toBytes("user"), Bytes.toBytes("weight"), Bytes.toBytes("60kg"));

        Put put4 = new Put(Bytes.toBytes("rowKey4"));
        put4.addColumn(Bytes.toBytes("user"), Bytes.toBytes("sex"), Bytes.toBytes("男"));

        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        table.put(puts);
        table.close();
    }

}
