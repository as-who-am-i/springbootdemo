package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Prigram: com
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-11 17:00
 */
//自定义分区
public class WCPartitioner extends Partitioner<Text, IntWritable> {
    //i表示分区的个数
    @Override
    public int getPartition(Text key, IntWritable value, int i) {
        String head = key.toString().substring(0, 1).toLowerCase();
        if (head.compareTo("m")>=0){
            return 0;
        }else {
            return 1;
        }
    }
}
