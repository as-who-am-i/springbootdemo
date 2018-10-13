package day04;

import com.*;
import com.google.common.io.Resources;
import day02.HomeWorkComb;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;


/**
 * @Prigram: day04
 * @Description:
 * 复合式MapReduce
 * 1、MapReduce有3个子任务job，job1，job2构成，其中job1和job2相互独立，
 *    job要在job1和job2完成后才能执行
 *    这样的关系就叫做复杂数据依赖关系的组合（有向无环图）
 * 2、Hadoop为这种组合挂席提供了一种执行和控制机制，Hadoop通过job和jobControl类提供具体的编程方法
 * 3、job除了维护子任务的配置信息，还维护子任务的依赖关系
 * 4、jobControl控制整个作业流程，把所有的子任务作业加入到jobControl的run()方法即可运行程序
 * @Author: DongFang
 * @CreaeteTime: 2018-10-12 12:28
 */
public class ChainJob {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-local.xml"));
        //设置两个子任务
        Job job1= setJob1();//将“/”目录下小于2M的文件合并成一个大文件
        Job job2= setJob2();//计算文件中单词出现的次数

        ControlledJob controlledJob1 = new ControlledJob(configuration);
        controlledJob1.setJob(job1);

        ControlledJob controlledJob2 = new ControlledJob(configuration);
        controlledJob2.setJob(job2);

        //子任务job2依赖于子任务job1
        controlledJob2.addDependingJob(controlledJob1);

        JobControl jobControl = new JobControl("job");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        //启动运用程序
        new Thread(jobControl).start();

        for (int i = 0; i < 9; i++) {
            List<ControlledJob> runningJobList = jobControl.getRunningJobList();
            System.out.println(runningJobList);
            Thread.sleep(5000);
        }
    }

    private static Job setJob2() throws IOException {
        Configuration coreSiteConf = new Configuration();
        //coreSiteConf.set("","");

        coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "wcoo");
        //设置job的运行类
        job.setJarByClass(WCJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCCombiner.class);
        job.setReducerClass(WCReducer.class);
        job.setNumReduceTasks(2);
        job.setPartitionerClass(WCPartitioner.class);
        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/out/merge"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));

        FileOutputFormat.setOutputPath(job, new Path("/out/wcout"));
        return job;
    }

    private static Job setJob1() throws IOException {
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-local.xml"));


        //设置一个任务
        Job job = Job.getInstance(configuration, "HW");
        //设置job的运行类 mrDemo/target/mrDemo-1.0-SNAPSHOT.jar
        job.setJarByClass(HomeWorkComb.class);
        //job.setJar("mrDemo/target/mrDemo-1.0-SNAPSHOT.jar");
        //设置map和reduce处理类
        job.setMapperClass(HomeWorkComb.CombMapper.class);
        job.setReducerClass(HomeWorkComb.ComReducer.class);

        //job.setNumReduceTasks(2);
        //设置map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        /**
         * 设置任务的输入路径
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
        FileOutputFormat.setOutputPath(job, new Path("/out/merge"));

        return job;
    }
}
