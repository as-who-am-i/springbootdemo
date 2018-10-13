package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Prigram: com
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-09 16:42
 */
//用来处理map任务
/**
 *  map接收的k-v键值对，输出的也是k-v键值对
 *
 *  第一个泛型表示：输入key的数据类型 输入的数据相对文件开头的偏移量（行号）
 *  第二个泛型表示：输入value的数据类型 输入的是一行文件内容
 *  第三个泛型表示：输出key的数据类型
 *  第四个泛型表示：输出value的数据类型
 *
 *  LongWritable 等价于Java中的long
 *  Text 等价于Java中String
 *
 *  **Writable是Hadoop定义的基本数据类型，相当于对Java中的数据类型做了一个封装，同时支持序列化
 */

public class WCMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    //map方法每次处理一行数据 会被循环调用
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        IntWritable one = new IntWritable(1);

        //遍历所正则到的单词
        for (int i = 0; i < words.length; i++) {
            Text keyOut = new Text(words[i]);
            //输出
            context.write(keyOut,one);
        }
    }
}
