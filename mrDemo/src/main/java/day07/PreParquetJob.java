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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Prigram: day07
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-18 11:24
 */
public class PreParquetJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        String writeSchema = "message example {\n" +
                "required binary ip;\n" +
                "required binary date;\n" +
                "required binary method;\n" +
                "required binary url;\n" +
                "required binary status;\n" +
                "}";

        configuration.set("parquet.example.schema", writeSchema);
        configuration.addResource(Resources.getResource("core-site-local.xml"));
        Job preParquet = Job.getInstance(configuration, "PreParquet");

        preParquet.setJarByClass(PreParquetJob.class);
        preParquet.setMapperClass(PreParquetMapper.class);
        preParquet.setReducerClass(PreParquetReducer.class);

        preParquet.setMapOutputKeyClass(Text.class);
        preParquet.setMapOutputValueClass(NullWritable.class);

        preParquet.setOutputFormatClass(ParquetOutputFormat.class);
        //preParquet.setMapOutputValueClass(Group.class);
        preParquet.setOutputValueClass(Group.class);

        FileInputFormat.addInputPath(preParquet, new Path("/show"));
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(new Path("/out_PreParquet/"))) {
            fileSystem.delete(new Path("/out_PreParquet/"), true);
        }

        ParquetOutputFormat.setOutputPath(preParquet, new Path("/out_PreParquet/"));

        ParquetOutputFormat.setWriteSupportClass(preParquet, GroupWriteSupport.class);

        //运行任务
        boolean flag = preParquet.waitForCompletion(true);
    }

    public static class PreParquetMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class PreParquetReducer extends Reducer<Text, NullWritable, Void, Group> {

        //声明
        private SimpleGroupFactory factory;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
        }

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String line = key.toString();
            String reg = "(\\d+\\.\\d+\\.\\d+\\.\\d+).*(\\d+\\/\\S+).*(\\\".*\\\")\\s(\\S+).*";
            //编译正则表达式
            Pattern pattern = Pattern.compile(reg);
            //将字符串根据正则表达式进行正则拆分
            Matcher matcher = pattern.matcher(line);
            //字符串与正则表达式相匹配返回true
            boolean matches = matcher.matches();

            if (matches && matcher.groupCount() == 4) {
                //ip,logindata,method,url,status,port
                String ip = matcher.group(1);
                String date = matcher.group(2);
                String path = matcher.group(3);
                String status = matcher.group(4);

                //对path拆分
                String[] strings = path.split(" ");
                if (strings.length >= 2) {
                    String method = strings[0].substring(1);
                    String url = strings[1];


                    //"message example {\n" +
                    //                "required binary ip;\n" +
                    //                "required binary date;\n" +
                    //                "required binary method;\n" +
                    //                "required binary url;\n" +
                    //                "required binary status;\n" +
                    //                "}";
                    Group group = factory.newGroup()
                            .append("ip", ip)
                            .append("date", date)
                            .append("method", method)
                            .append("url", url)
                            .append("status", status);

                    context.write(null, group);
                }
            }

        }

    }


}
