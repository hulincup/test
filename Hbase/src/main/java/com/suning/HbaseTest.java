package com.suning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author lynn
 */
public class HbaseTest {

    //获取configure对象
    public static Configuration conf;
    public static  Connection connection;
    public static HBaseAdmin admin;
    static{
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bd1301,bd1302,bd1303");
        //可以不需要指定端口，在hbase-default.xml有默认zookeeper端口号
        //conf.set("hbase.zookeeper.property.clientPort", "2181");
        //在HBase中管理、访问表需要先创建HBaseAdmin对象

        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 判断表明是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(tableName);
    }

    /**
     * 判断表明是否存在
     * @param tableName
     * @param columnFamily 可变参数
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException{
        //判断表是否存在
        if(isTableExist(tableName)){
            System.out.println("表" + tableName + "已存在");
        }else{
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for(String cf : columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
    }

    /**
     * 删除表
     * @param tableName
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException{

        if(isTableExist(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        }else{
            System.out.println("表" + tableName + "不存在！");
        }
    }


    /**
     * 向表中插入数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String
            column, String value) throws IOException{
        //创建Table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //向Put对象赋值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        //插入数据
        table.put(put);
        //关闭表连接
        table.close();
        System.out.println("插入数据成功");
    }

    /**
     * 获取某行数据
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException{
        //创建Table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());

            //关闭表连接
            table.close();
        }
    }

    /**
     * 获取指定列族,指定列的数据
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void getRowQualifier(String tableName, String rowKey, String family, String
            qualifier) throws IOException{
        //创建Table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        //关闭表连接
        table.close();
    }

    /**
     * 删除多行数据
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void deleteMultiRow(String tableName, String... rows) throws IOException{
        //创建Table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<Delete>();
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }

    public static void main(String[] args) throws IOException {
        //判断表明是否存在
        System.out.println(isTableExist("student"));

        //创建表
        createTable("person","info01","info02");

        //删除表
        dropTable("person");

        //插入数据

        addRowData("student","1001","info","name","lisi");

        //获取某行数据
        getRow("student","1001");

        //获取指定列族指定列的数据
        getRowQualifier("student","1001","info","name");

        //删除多行数据
        deleteMultiRow("student","1001","1002");

    }


}
