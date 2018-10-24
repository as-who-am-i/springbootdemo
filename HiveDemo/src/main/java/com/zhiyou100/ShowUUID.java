package com.zhiyou100;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.UUID;

/**
 * @Prigram: com.zhiyou100
 * @Description:
 * hive 自定义函数
 * 第一步：导入hive的hive-exec和hive-common两个jar包，
 *        继承udf（user define function）
 * 第二部：实现evaluate()
 * 第三部：打包编译上传
 *        1、在hive环境下添加以编译好的jar包
 *           add jar jar'path;
 *        2、查看jar包
 *           list jars;
 *        3、创建临时自定义方法
 *           create temporary function uuid as 'class'path';
 *
 *
 *
 * @Author: DongFang
 * @CreaeteTime: 2018-10-22 10:50
 */

public class ShowUUID extends UDF {


    public String evaluate(){
        return UUID.randomUUID().toString();
    }

}
