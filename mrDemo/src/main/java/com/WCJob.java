package com;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
//mapred是Hadoop 1.x的api
//mapreduce是Hadoop 2.x的api

/**
 * @Prigram: com
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-09 17:15
 */
public class WCJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-local.xml"));

        //设置一个任务
        Job job = Job.getInstance(configuration, "wc");
        //设置job的运行类 mrDemo/target/mrDemo-1.0-SNAPSHOT.jar
        job.setJarByClass(WCJob.class);
        //job.setJar("mrDemo/target/mrDemo-1.0-SNAPSHOT.jar");
        //设置map和reduce处理类
        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCCombiner.class);
        job.setReducerClass(WCReducer.class);

        //设置reduce的数量
        job.setNumReduceTasks(2);

        //自定义分区
        job.setPartitionerClass(WCPartitioner.class);

        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job,new Path("/wc/"));
        FileOutputFormat.setOutputPath(job,new Path("/wcout115"));

        //运行任务
        job.waitForCompletion(true);

    }
}
