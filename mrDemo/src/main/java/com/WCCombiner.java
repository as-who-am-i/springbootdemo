package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Prigram: com
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-09 17:00
 */

//用来处理reduce任务，在reduce端 框架会将相同的key的value放在一个集合中
public class WCCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {

    //reduce每次会处理一个key 会被循环利用
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int count=0;
        while (iterator.hasNext()){
            IntWritable one = iterator.next();
            count+=one.get();
        }
        //context 的write只接受Hadoop数据类型，不能输出Java的数据类型
        context.write(key,new IntWritable(count));
    }
}
