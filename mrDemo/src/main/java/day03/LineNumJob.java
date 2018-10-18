package day03;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Prigram: day03
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-11 14:32
 */
//读取文件的行数，将行数保存到MySQL数据库中
public class LineNumJob {
    public static class LineNumMapper extends Mapper<LongWritable, Text,Text,NullWritable>{

        Connection connection;
        PreparedStatement preparedStatement;
        //在进行业务处理前连接数据库,即先获取数据的连接
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection("jdbc:mysql://192.168.203.35:3306/test", "root", "902717");
                preparedStatement = connection.prepareStatement("update line_num set `count`=`count`+1 where id=1");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                boolean b = preparedStatement.execute();
                context.write(new Text(), NullWritable.get());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        //在业务处理后进行数据库的关闭
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try {
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-remote.xml"));
        Job job = Job.getInstance(configuration, "HW");

        //设置job的运行类
        //job.setJarByClass(LineNumJob.class);
        job.setJar("mrDemo/target/mrDemo-1.0-SNAPSHOT.jar");

        //设置Map的处理类
        job.setMapperClass(LineNumMapper.class);

        //map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job,new Path("/homework"));

        FileOutputFormat.setOutputPath(job,new Path("/lineOut1111"));

        job.waitForCompletion(true);
    }
}
