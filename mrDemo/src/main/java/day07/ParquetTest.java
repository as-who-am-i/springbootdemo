package day07;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class ParquetTest {
    static Logger logger = Logger.getLogger(ParquetTest.class);

    public static void main(String[] args) throws Exception {

        //parquetWriter("d:\\test\\parquet-out2","d:\\test\\city-ip.txt");
        parquetReaderV2("e:\\test\\test\\res1.parquet");
        //makeData();

    }

    private static void makeData() throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message Pair {\n" +
                //ip+","+data+","+method+","+path+","+status+","+port
                " required binary ip (UTF8);\n" +
                " required binary logindata (UTF8);\n" +
                " required binary method (UTF8);\n" +
                " required binary url (UTF8);\n" +
                " required binary status (UTF8);\n" +
                " required binary port (UTF8);\n" +
                "}");


        GroupFactory factory = new SimpleGroupFactory(schema);

        Path path = new Path("e:\\test\\test\\res1.parquet");

        Configuration configuration = new Configuration();

        GroupWriteSupport writeSupport = new GroupWriteSupport();

        writeSupport.setSchema(schema, configuration);



        ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, writeSupport);
        //把本地文件读取进去，用来生成parquet格式文件
        BufferedReader br = new BufferedReader(new FileReader(new File("E:\\test\\test\\part-r-00000")));

        String line = "";
        Random r = new Random();
        while ((line = br.readLine()) != null) {
            String[] strs = line.split(",");
            if (strs.length == 5) {
                Group group = factory.newGroup()
                        //1.161.126.1,8/April/2016:21:01:50,GET,/api.php?mod=js&bid=65,200,1414
                        .append("ip", strs[0])
                        .append("logindata", strs[1])
                        .append("method", strs[2])
                        .append("url", strs[3])
                        .append("status", strs[4])
                        .append("port", strs[5]);
                writer.write(group);

            }
        }
        System.out.println("write end");
        writer.close();
    }


    static void parquetReaderV2(String inPath) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();

        Builder<Group> builder = ParquetReader.builder(readSupport, new Path(inPath));

        ParquetReader<Group> reader = builder.build();
        Group line = null;
        while ((line = reader.read()) != null) {
            Group time = line.getGroup("time", 0);
//通过下标和字段名称都可以获取
/*System.out.println(line.getString(0, 0)+"\t"+
line.getString(1, 0)+"\t"+
time.getInteger(0, 0)+"\t"+
time.getString(1, 0)+"\t");*/
            System.out.println(line.getString("city", 0) + "\t" +
                    line.getString("ip", 0) + "\t" +
                    time.getInteger("ttl", 0) + "\t" +
                    time.getString("ttl2", 0) + "\t");

        }
        System.out.println("读取结束");
    }

    //新版本中new ParquetReader()所有构造方法好像都弃用了,用上面的builder去构造对象
    static void parquetReader(String inPath) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = new ParquetReader<Group>(new Path(inPath), readSupport);
        Group line = null;
        while ((line = reader.read()) != null) {
            System.out.println(line.toString());
        }
        System.out.println("读取结束");

    }

    /**
     * @param outPath 输出Parquet格式
     * @param inPath             输入普通文本文件
     * @throws IOException
     */
    static void parquetWriter(String outPath, String inPath) throws IOException {

        MessageType schema = MessageTypeParser.parseMessageType("message Pair {\n" +
                " required binary city (UTF8);\n" +
                " required binary ip (UTF8);\n" +
                " repeated group time {\n" +
                " required int32 ttl;\n" +
                " required binary ttl2;\n" +
                "}\n" +
                "}");


        GroupFactory factory = new SimpleGroupFactory(schema);

        Path path = new Path(outPath);

        Configuration configuration = new Configuration();

        GroupWriteSupport writeSupport = new GroupWriteSupport();

        writeSupport.setSchema(schema, configuration);



        ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, writeSupport);
//把本地文件读取进去，用来生成parquet格式文件
        BufferedReader br = new BufferedReader(new FileReader(new File(inPath)));
        String line = "";
        Random r = new Random();
        while ((line = br.readLine()) != null) {
            String[] strs = line.split("\\s+");
            if (strs.length == 2) {
                Group group = factory.newGroup()
                        .append("city", strs[0])
                        .append("ip", strs[1]);
                Group tmpG = group.addGroup("time");
                tmpG.append("ttl", r.nextInt(9) + 1);
                tmpG.append("ttl2", r.nextInt(9) + "_a");
                writer.write(group);
            }
        }
        System.out.println("write end");
        writer.close();
    }
}