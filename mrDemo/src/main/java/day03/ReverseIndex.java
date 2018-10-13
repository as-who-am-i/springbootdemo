package day03;


import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Prigram: day03
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-10 14:29
 */
public class ReverseIndex {

    //倒排索引
    public static class RIMapper extends Mapper<LongWritable,Text,Text,Text> {

        //map方法每次处理一行数据 会被循环调用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            //Path path = fileSplit.getPath();

            String line = value.toString();
            String[] words = line.split(" ");

            //遍历所正则到的单词
            for (int i = 0; i < words.length; i++) {
                Text keyOut = new Text(words[i]);
                //输出
                context.write(keyOut,new Text(fileName));
                //context.write(keyOut,new Text(path.toString()));
            }
        }
    }

    public static class RIReducer extends Reducer<Text,Text,Text,Text> {

        //reduce每次会处理一个key 会被循环利用
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            StringBuilder stringBuilder = new StringBuilder();
            while (iterator.hasNext()){
                Text text = iterator.next();
                stringBuilder.append(" ").append(text);
            }
            //context 的write只接受Hadoop数据类型，不能输出Java的数据类型
            context.write(key,new Text(stringBuilder.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-local.xml"));

        //设置一个任务
        Job job = Job.getInstance(configuration, "RI");
        //设置job的运行类 mrDemo/target/mrDemo-1.0-SNAPSHOT.jar
        //job.setJarByClass(WCJob.class);
        job.setJar("mrDemo/target/mrDemo-1.0-SNAPSHOT.jar");
        //设置map和reduce处理类
        job.setMapperClass(RIMapper.class);
        job.setReducerClass(RIReducer.class);

        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job,new Path("/wc/"));
        FileOutputFormat.setOutputPath(job,new Path("/wcout101"));

        //运行任务
        job.waitForCompletion(true);
    }
}
