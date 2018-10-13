package day03;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @Prigram: day03
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-10 15:50
 */
//构建联表查询，获取相同的列，将相对应的值连接在一起组成新的数据表
public class Join {

    //倒排索引
    public static class JoinMapper extends Mapper<LongWritable,Text,Text,Text> {

        //map方法每次处理一行数据 会被循环调用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();

            String[] words = value.toString().split(",");

            if (fileName.contains("class")) {
                context.write(new Text(words[0]),new Text(words[1]));
            }else{
                context.write(new Text(words[2]),new Text(words[0]+","+words[1]));
            }

        }
    }

    public static class JoinReducer extends Reducer<Text,Text,Text, NullWritable> {

        //reduce每次会处理一个key 会被循环利用
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> arrayList = new ArrayList<String>();
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()){
                Text text = iterator.next();
                arrayList.add(text.toString());
            }
            //context 的write只接受Hadoop数据类型，不能输出Java的数据类型
            //String tmp="";
            StringBuilder tmp=new StringBuilder();
            for (int i = 0; i < arrayList.size(); i++) {
                if (!arrayList.get(i).contains(",")) {
                    //tmp=arrayList.get(i);
                    tmp.append(arrayList.get(i));
                    break;
                }
            }
            for (int i = 0; i < arrayList.size(); i++) {
                if (arrayList.get(i).contains(",")){
                    context.write(new Text(arrayList.get(i)+","+tmp),NullWritable.get());
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-local.xml"));

        //设置一个任务
        Job job = Job.getInstance(configuration, "RI");
        //设置job的运行类 mrDemo/target/mrDemo-1.0-SNAPSHOT.jar
        job.setJarByClass(Join.class);
        //job.setJar("mrDemo/target/mrDemo-1.0-SNAPSHOT.jar");
        //设置map和reduce处理类
        job.setMapperClass(Join.JoinMapper.class);
        job.setReducerClass(Join.JoinReducer.class);

        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job,new Path("/join/"));
        FileOutputFormat.setOutputPath(job,new Path("/joinout21"));

        //运行任务
        job.waitForCompletion(true);
    }
}
