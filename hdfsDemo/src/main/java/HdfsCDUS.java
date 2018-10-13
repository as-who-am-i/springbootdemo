import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @Prigram: PACKAGE_NAME
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-09 10:42
 */
public class HdfsCDUS {
    public static void main(String[] args) throws IOException {
        //创建文件
        //fileCreate();

        //删除文件
        //deleteFile();

        //查看文件
        catFile();
    }

    private static void catFile() throws IOException {
        //加载配置文件
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));

        //获取文件系统
        FileSystem fileSystem = FileSystem.get(coreSiteConf);

        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/java/test.txt"));

        StringBuilder stringBuilder = new StringBuilder();
        while (fsDataInputStream.read()==-1){
            stringBuilder.append(fsDataInputStream.read());
        }
        System.out.println(stringBuilder.toString());

        fsDataInputStream.close();
        fileSystem.close();
    }

    private static void deleteFile() throws IOException {
        //加载配置文件
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));

        //获取文件系统
        FileSystem fileSystem = FileSystem.get(coreSiteConf);

        //添加文件及内容
        boolean delete = fileSystem.delete(new Path("/java/test.txt"), true);

        System.out.println(delete);

        //关闭流和文件系统

        fileSystem.close();
    }

    private static void fileCreate() throws IOException {
        //加载配置文件
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));

        //获取文件系统
        FileSystem fileSystem = FileSystem.get(coreSiteConf);

        //添加文件及内容
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/java/test.txt"));
        fsDataOutputStream.write("Hello World!".getBytes());

        //关闭流和文件系统
        fsDataOutputStream.close();
        fileSystem.close();
    }
}
