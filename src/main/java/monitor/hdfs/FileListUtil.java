package monitor.hdfs;

import java.io.IOException;
import java.util.HashSet;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Created with IntelliJ IDEA
 *
 * @Auther :yangwang
 * Data:2021/1/7
 * Time:10:26
 * @Description:
 */
public class FileListUtil {

    static HashSet hashSet = null;

    static {
        hashSet = new HashSet<String>();
    }

    public static HashSet filesList(String path) {
        Configuration conf = new Configuration();

        try {
            // 获取文件系统
            FileSystem fileSystem = FileSystem.get(URI.create(path), conf);
            // 文件状态
            FileStatus[] status = fileSystem.listStatus(new Path(path));
            // listPath
            Path[] paths = FileUtil.stat2Paths(status);

            for (Path path1 : paths) {
                if (fileSystem.getFileStatus(path1).isFile()) {
                    String substring = String.valueOf(path1).substring(19, String.valueOf(path1).lastIndexOf("/"));
                    String s = substring.substring(0, substring.lastIndexOf("/"));
                    hashSet.add(s);
                } else {
                    filesList(path1.toString());
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hashSet;
    }

    public static void main(String[] args) {
        HashSet set = filesList("/data2");
        for (Object o : set) {
            System.out.println(o);
        }
//        /data2/dcx/dhlk_tenant_207333/2020-08
//                /dhlk/dhlk_lighting/dhlk_tenant_135809
    }
}
