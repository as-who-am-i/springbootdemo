package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.shaded.org.apache.commons.configuration.ConfigurationFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Prigram: com
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-25 16:39
 */



public class PhoenixJDBC {

    private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static Statement stmt ;
    private static ResultSet rs ;
    private static Connection con;
    //private static Configuration conf;
    static {
        try {
            Class.forName(driver);
            con = DriverManager.getConnection("jdbc:phoenix:single:2181");

            stmt = PhoenixJDBC.con.createStatement();



           /* conf =HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.186.111");*/
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {

        //表名
        String table_name="test";
        List<String> list = new ArrayList<String>();
        list.add("id");
        list.add("name");
        list.add("age");
        Map<String, String> map = new HashMap();
        map.put("3","梁山伯");
        map.put("4","祝英台");

        //创建表
        //createTable(table_name);

        //insert(table_name,map);

        //查询表
        queryALL(table_name);

        //删除表
        //dropTable(table1);

    }





    private static void insert(String tableName, Map<String, String> map) {
        //获取表



    }

    private static void dropTable(String tableName) {

    }

    private static void createTable(String tableName,String... familyColumn) throws SQLException {
        //TableName name = TableName.create(schema, tableName);
        String sql="CREATE TABLE IF NOT EXISTS test1(SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);";

        /*StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append("( ");
        for (int i = 0; i < familyColumn.length; i++) {
            if (i == 0) {
                sql.append(familyColumn[i]+" varchar not null primary key,");

            }else if (i<(familyColumn.length-1)){
                sql.append(familyColumn[i]+" varchar,");
            }else {
                sql.append(familyColumn[i]+" varchar);");
            }
        }*/


        //int flag =stmt.executeUpdate(sql);
        boolean flag = stmt.execute(sql);
        if (flag) {
            System.out.println(tableName+"表 创建成功");
        }else{
            System.out.println("创建失败");
        }
        stmt.close();
        con.close();

    }

    /**
     * CREATE TABLE IF NOT EXISTS STOCK_SYMBOL (SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR);
     * UPSERT INTO STOCK_SYMBOL VALUES ('CRM','SalesForce.com');
     */
    private static void queryALL(String tableName) throws SQLException {
        String sql = "select * from "+tableName;
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.print("id:"+rs.getString("id"));
            System.out.println(",name:"+rs.getString("name"));
        }
        stmt.close();
        con.close();
    }
}
