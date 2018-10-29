package com.day01;

import static org.junit.Assert.*;


import java.util.Map;

import org.junit.Test;

public class HBaseUtilTest {

    @Test
    public void testCreateTable() {
        //创建表
        HBaseUtil.createTable("myTest", "myfc1", "myfc2", "myfc3");
        HBaseUtil.close();
        HBaseUtil.createTable("myTest02", "myfc1", "myfc2", "myfc3");
        HBaseUtil.close();
    }



    @Test
    public void testInsert() {
        //插入数据
        /*HBaseUtil.insert("myTest", "1", "myfc1", "sex", "men");
        HBaseUtil.insert("myTest", "1", "myfc1", "name", "xiaoming");
        HBaseUtil.insert("myTest", "1", "myfc1", "age", "32");
        HBaseUtil.insert("myTest", "1", "myfc2", "name", "xiaohong");
        HBaseUtil.insert("myTest", "1", "myfc2", "sex", "woman");
        HBaseUtil.insert("myTest", "1", "myfc2", "age", "23");*/
        HBaseUtil.insert("myTest", "1", "myfc3", "name", "xiaozhang");
        HBaseUtil.insert("myTest", "1", "myfc3", "age", "23");
        HBaseUtil.insert("myTest", "1", "myfc3", "sex", "woman");

    }

    @Test
    public void testByGet() {
        //得到一行下一个列族下的某列的数据
        String result = HBaseUtil.byGet("myTest", "1", "myfc1", "name");
        System.out.println("结果是的： " + result);
        //assertEquals("xiaosan", result);
    }

    @Test
    public void testByGet02() {
        //得到一行下一个列族下的所有列的数据
        Map<String, String> result = HBaseUtil.byGet("myTest", "1", "myfc1");
        System.out.println("结果是的： " + result);
        assertNotNull(result);
    }

    @Test
    public void testByGet03() {
        //得到一行的所有列族的数据
        Map<String, Map<String, String>> result = HBaseUtil.byGet("myTest", "1");

        System.out.println("所有列族的数据是:  " + result);
        System.out.println("结果是的： " + result.get("myfc1"));
        assertNotNull(result);
    }

    @Test
    public void testDropTable() {
        //删除表
        HBaseUtil.dropTable("myTest");
        HBaseUtil.dropTable("myTest02");
    }
}


