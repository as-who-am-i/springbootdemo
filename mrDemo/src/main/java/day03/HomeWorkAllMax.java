package day03;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * <p>
 * 用mr求每个省的最大销售额
 * 用mr求每个省的平均销售额
 * 用mr最大销售额的城市
 * @Author: DongFang
 * @CreaeteTime: 2018-10-10 15:50
 */
//构建联表查询，获取相同的列，将相对应的值连接在一起组成新的数据表
public class HomeWorkAllMax {

    //倒排索引
    public static class HWMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        //map方法每次处理一行数据 会被循环调用
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] words = value.toString().split(",");

            context.write(NullWritable.get(), new Text(words[1] + "," + words[2]));

        }
    }

    public static class HWReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

        //reduce每次会处理一个key 会被循环利用
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> arrayList = new ArrayList<String>();
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                Text text = iterator.next();
                arrayList.add(text.toString());
            }
            int max = 0;
            String city = "";
            for (int i = 0; i < arrayList.size(); i++) {
                String[] date = arrayList.get(i).split(",");
                if (Integer.parseInt(date[1]) > max) {
                    max = Integer.parseInt(date[1]);
                    city = date[0];
                }
            }
            context.write(new Text(city), new IntWritable(max));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-local.xml"));

        //设置一个任务
        Job job = Job.getInstance(configuration, "HW");
        //设置job的运行类 mrDemo/target/mrDemo-1.0-SNAPSHOT.jar
        job.setJarByClass(HomeWorkAllMax.class);
        //job.setJar("mrDemo/target/mrDemo-1.0-SNAPSHOT.jar");
        //设置map和reduce处理类
        job.setMapperClass(HomeWorkAllMax.HWMapper.class);
        job.setReducerClass(HomeWorkAllMax.HWReducer.class);

        //设置map的输出类型
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/homework/"));
        FileOutputFormat.setOutputPath(job, new Path("/homeworkAllMaxOut"));

        //运行任务
        job.waitForCompletion(true);
    }
}
