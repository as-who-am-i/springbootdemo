package com.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Prigram: com.filter
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-26 10:23
 */
public class HBaseFilterTest {

    private Configuration conf;
    private Connection conn;

    @Before
    public void init() throws IOException {
        conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","single");

        conn= ConnectionFactory.createConnection(conf);
    }

    @Test
    public void testRowFilter() throws IOException {
        //创建行比较器 select  from web_stat where rowkey=1;
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("1".getBytes()));
        //创建查询
        Scan scan = new Scan();
        scan.setFilter(rowFilter);

        Table web_stat = conn.getTable(TableName.valueOf("myTest"));

        ResultScanner scanner = web_stat.getScanner(scan);

        for (Result rs:
             scanner) {
            byte[] rsValue = rs.getValue(Bytes.toBytes("myfc1"), Bytes.toBytes("sex"));
            String rsString = Bytes.toString(rsValue);
            System.out.println(rsString);
        }
        conf.clear();
        conn.close();
        web_stat.close();

    }
}
