package com;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseCURD {

	private Configuration conf = null;
    private Connection con = null;
    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.186.111:2181");
    }
    @Test
    public void testCreateTable() throws IOException {
    	HBaseAdmin admin = new HBaseAdmin(conf);       
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("people"));
        HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
        HColumnDescriptor hcdData = new HColumnDescriptor("data");
        hcdInfo.setMaxVersions(3);
        htd.addFamily(hcdInfo);
        htd.addFamily(hcdData);
        admin.createTable(htd);
        //����ر�
        admin.close();
    }
    /**��������
     * @throws IOException **/
    @Test
    public void testPut(String tableName) throws IOException {
    	HTable table = new HTable(conf, tableName);    
        Put put = new Put(Bytes.toBytes("row001"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lingxin"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("57"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(30000));
        table.put(put);
        table.close();
    }
    /**���Բ���100��������**/
    @Test
    public void testPutAll() throws IOException {
    	HTable table = new HTable(conf, "people");    
        List<Put> puts = new ArrayList<Put>(10000);
        for(int i = 1; i < 1000000; i++) {
            Put put = new Put(Bytes.toBytes("row" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lingxin" + i));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("57"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(30000));
            puts.add(put);
            //ÿ��10000,дһ��
            if(i % 10000 == 0) {
                table.put(puts);
                puts = new ArrayList<Put>(10000);
            }
        }
        table.put(puts);
        table.close();
        //���·�ʽ����ȡ
        /*      for(int i=1; i<1000000; i++) {
                    Put put = new Put(Bytes.toBytes("row"+i));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lingxin"+i));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("57"));
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(30000));
                    puts.add(put);
                }
                table.put(puts);
                table.close();*/
    }
    /**�鿴ĳ��cell��ֵ**/
    @Test
    public void testGet() throws IOException {
        Table table = con.getTable(TableName.valueOf("people"));
        Get get = new Get(Bytes.toBytes("row9999"));
        Result resut = table.get(get);
        String r = Bytes.toString(resut.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
        System.out.println(r);
        table.close();
    }
    /**�鿴ĳ��rowkey��Χ�����ݣ����ֵ�˳������**/
    @Test
    public void testScan() throws IOException {
        Table table = con.getTable(TableName.valueOf("people"));
        Scan scan = new Scan(Bytes.toBytes("row010000"), Bytes.toBytes("row110"));
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner) {
            String r = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            System.out.println(r);
        }
        table.close();
    }
    /**���ʹ��ɨ���������������С**/
    @Test
    public void testScanWithCacheAndBatch(int caching, int batch) throws IOException {
        Table table = con.getTable(TableName.valueOf("people"));
        Scan scan = new Scan(Bytes.toBytes("row010000"), Bytes.toBytes("row110"));
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        scan.setCaching(caching);	
        scan.setBatch(batch); 
        //!!!!!!!!!!!!!!!!!!!!!!!!!!
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner) {
            String r = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            System.out.println(r);
        }
        table.close();
    }
    @Test
    public void testDel() throws IOException {
        Table table = con.getTable(TableName.valueOf("people"));//	HTable table = new HTable(conf, "people");  
        Delete delete = new Delete(Bytes.toBytes("row9999"));
        table.delete(delete);
        table.close();
    }


}
