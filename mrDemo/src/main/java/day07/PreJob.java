package day07;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Prigram: day07
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-17 13:11
 */
public class PreJob {
    public static class PreMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //110.75.173.48 - - [28/April/2016:23:59:58 +0800] "GET /thread-36410-1-9.html HTTP/1.1" 200 68628
            String reg = "(\\d+\\.+\\d+\\.+\\d+\\.+\\d).*" +
                    "(\\d+\\/\\S+).*" +
                    "(\\\".*\\\")\\s" +
                    "(\\S+)\\s" +
                    "(\\S+)";
            Pattern compile = Pattern.compile(reg);
            Matcher matcher = compile.matcher(line);
            if (matcher.matches()&&matcher.groupCount()==5){
                String ip = matcher.group(1);
                String data = matcher.group(2);
                String url= matcher.group(3);
                String status = matcher.group(4);
                String prot = matcher.group(5);

                String[] strings = url.split(" ");
                if (strings.length>=2){
                    String method = strings[0].substring(1);
                    String path = strings[1];
                    context.write(new Text(ip+","+data+","+method+","+path+","+status+","+prot),NullWritable.get());
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();

        coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "pre");
        //设置job的运行类
        job.setJarByClass(PreJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(PreMapper.class);
        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置job/reduce输出类型
        /*job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);*/

        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/show/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        if (fileSystem.exists(new Path("/out/"))) {
            fileSystem.delete(new Path("/out/"), true);
        }


        FileOutputFormat.setOutputPath(job, new Path("/out/"));
        //运行任务
        boolean flag = job.waitForCompletion(true);
    }
}
