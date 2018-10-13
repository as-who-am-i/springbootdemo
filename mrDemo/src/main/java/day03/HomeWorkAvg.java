package day03;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @Prigram: day03
 * @Description: TODO
 *
    用mr求每个省的最大销售额
    用mr求每个省的平均销售额
    用mr最大销售额的城市
 *
 * @Author: DongFang
 * @CreaeteTime: 2018-10-10 15:50
 */
public class HomeWorkAvg {

    public static class HWMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

        //map方法每次处理一行数据 会被循环调用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] words = value.toString().split(",");

            for (int i = 0; i < words.length; i++) {
                int parseInt = Integer.parseInt(words[2]);
                IntWritable intWritable = new IntWritable(parseInt);
                context.write(new Text(words[0]),intWritable);
            }

        }
    }

    public static class HWReducer extends Reducer<Text,IntWritable,Text, IntWritable> {

        //reduce每次会处理一个key 会被循环利用
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<Integer> arrayList = new ArrayList();
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                IntWritable intWritable = iterator.next();
                arrayList.add(intWritable.get());
            }
            //context 的write只接受Hadoop数据类型，不能输出Java的数据类型
            int avg=0;
            for (int i = 0; i < arrayList.size(); i++) {
                avg+=arrayList.get(i);
            }
            avg=avg/arrayList.size();
            context.write(key,new IntWritable(avg));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-local.xml"));

        //设置一个任务
        Job job = Job.getInstance(configuration, "HW");
        //设置job的运行类 mrDemo/target/mrDemo-1.0-SNAPSHOT.jar
        job.setJarByClass(HomeWorkAvg.class);
        //job.setJar("mrDemo/target/mrDemo-1.0-SNAPSHOT.jar");
        //设置map和reduce处理类
        job.setMapperClass(HomeWorkAvg.HWMapper.class);
        job.setReducerClass(HomeWorkAvg.HWReducer.class);

        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job,new Path("/homework/"));
        FileOutputFormat.setOutputPath(job,new Path("/homeworkAvgOut1"));

        //运行任务
        job.waitForCompletion(true);
    }
}
