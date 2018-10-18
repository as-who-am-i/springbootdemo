package day05;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Prigram: day05
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-15 11:36
 */
public class ScoreJob {
    public static class ScoreMapper extends Mapper<LongWritable, Text,ScoreWritable, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] messages = value.toString().split(",");
            ScoreWritable scoreWritable = new ScoreWritable(Integer.parseInt(messages[0]),
                    Integer.parseInt(messages[1]),
                    Integer.parseInt(messages[2]),
                    Integer.parseInt(messages[3]));
            context.write(scoreWritable,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-local.xml"));
        //设置一个任务
        Job score = Job.getInstance(configuration, "score");
        score.setJarByClass(ScoreJob.class);
        score.setMapperClass(ScoreMapper.class);

        //设置map的输出类型
        score.setMapOutputKeyClass(ScoreWritable.class);
        score.setOutputValueClass(NullWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(score,new Path("/score"));
        //设置任务的输出路径
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/out/scoreOut");
        if (fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }
        FileOutputFormat.setOutputPath(score,path);

        //运行任务
        boolean flag = score.waitForCompletion(true);

        //输出结果
        if (flag){
            FSDataInputStream open = fileSystem.open(new Path("/out/scoreOut/part-r-00000"));
            byte[] buffer = new byte[1024];
            //open.read(buffer,0,open.available());
            IOUtils.readFully(open,buffer,0,open.available());
            System.out.println(new String(buffer));
        }

    }
}
