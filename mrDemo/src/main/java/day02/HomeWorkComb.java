package day02;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * @Prigram: day02
 * @Description: 写一个mr实现小文件的合并
 * 要求：
 * 1.扫描hdfs上的文本文件
 * 2.小文件的判定标准是小于2MB
 * 3,合并小文件成一个大文件。
 * 合并的方式是每个小文件的内容为一行。如果小文件为多行文件，则将小文件拼成字符串。
 * 一行由key:filename,value：content组成。
 * 如：
 * a.txt  的内容：
 * hello henan
 * hello world
 * b.txt  的内容：
 * hello henan
 * hello zhengzhou
 * <p>
 * 结果：
 * a.txt:hello henan hello world
 * b.txt:hello henan hello zhengzhou
 * @Author: DongFang
 * @CreaeteTime: 2018-10-11 21:04
 */
public class HomeWorkComb {

    public static class CombMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            context.write(new Text(fileName), new Text(value));
        }

    }


    public static class ComReducer extends Reducer<Text, Text, Text, NullWritable> {

        //reduce每次会处理一个key 会被循环利用
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(key).append(":");
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                Text text = iterator.next();
                stringBuilder.append(" ").append(text);
            }
            context.write(new Text(stringBuilder.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-local.xml"));


        //设置一个任务
        Job job = Job.getInstance(configuration, "HW");
        //设置job的运行类 mrDemo/target/mrDemo-1.0-SNAPSHOT.jar
        job.setJarByClass(HomeWorkComb.class);
        //job.setJar("mrDemo/target/mrDemo-1.0-SNAPSHOT.jar");
        //设置map和reduce处理类
        job.setMapperClass(CombMapper.class);
        job.setReducerClass(ComReducer.class);

        //job.setNumReduceTasks(2);
        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        /**
         * 设置任务的输入路径
         *
         */
        FileSystem fileSystem = FileSystem.get(configuration);
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            if (!fileStatus.isDirectory()) {
                //判断文件的大小和格式
                if (fileStatus.getLen() < 2 * 1024 * 1024 && fileStatus.getPath().getName().contains(".txt")) {
                    FileInputFormat.addInputPath(job, fileStatus.getPath());
                }
            }

        }

        FileOutputFormat.setOutputPath(job, new Path("/out/homeworkOut"));

        //运行任务
        job.waitForCompletion(true);
    }
}
