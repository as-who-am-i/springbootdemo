package com.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Prigram: com.filter
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-26 13:08
 */
public class FilterUtil {

    private static Configuration conf;
    private static Connection conn;

    static {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "single");

            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void endClose(Table connTable) throws IOException {
        conn.close();
        connTable.close();
    }

    private static void searchResult(ResultScanner results,String... cf) {
        for (Result result:
                results) {
            for (int i = 0; i < cf.length; i++) {

                String res = Bytes.toString(result.getValue(Bytes.toBytes(cf[i]), Bytes.toBytes(cf[i+1])));
                System.out.println(res);
                i++;
            }
        }

    }

    //通过比较，过滤查询
    public static void equalFilter(String tableName,String rowKey,String... cf) throws IOException {
        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(rowKey.getBytes()));
        Scan scan = new Scan();
        scan.setFilter(filter);
        //获取表
        Table connTable = conn.getTable(TableName.valueOf(tableName));
        ResultScanner results = connTable.getScanner(scan);

        searchResult(results,cf);

        endClose(connTable);

    }



    //列名过滤
    public static void qualifierFilter(String tableName,String... cf) throws IOException {

        //select sex from stu;
        Scan scan = new Scan();
        FilterList filters = new FilterList();
        for (int i = 0; i < cf.length; i++) {
            QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(cf[i+1].getBytes()));
            filters.addFilter(qualifierFilter);
            i++;
        }
        scan.setFilter(filters);

        Table connTable = conn.getTable(TableName.valueOf(tableName));
        ResultScanner results = connTable.getScanner(scan);

        searchResult(results,cf);

        endClose(connTable);

    }

    //联合查询
    public static void combineFilter(String tableName,String rowKey,String... cf) throws IOException {


        Scan scan = new Scan();
        FilterList filters = new FilterList();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(rowKey)));
        filters.addFilter(rowFilter);
        for (int i = 0; i < cf.length; i++) {
            QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(cf[i+1].getBytes()));
            filters.addFilter(qualifierFilter);
            i++;
        }
        scan.setFilter(filters);
        Table connTable = conn.getTable(TableName.valueOf(tableName));
        ResultScanner results = connTable.getScanner(scan);

        searchResult(results,cf);

        endClose(connTable);

    }
}
