package monitor.hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.sound.midi.Soundbank;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA
 *
 * @Auther :yangwang
 * Data:2021/1/8
 * Time:8:54
 * @Description:
 */
public class HdfsIncrementMonitor {
    // submit shell
    /*
     * main类的路径不需要指定，否则会被认为是参数传递进入。
     * yarn jar /app/m_user1/service/Hangzhou_HdfsFileMananger.jar /hive_tenant_account/hivedbname/
     */

    static HashSet hashSet = null;

    static {
        hashSet = new HashSet<Object>();
    }

    public static void main(String[] args) throws Exception {
        Connection conn = DBUtil.getConnection();
        // 获取昨天日期，当作参数传入
        String date = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis() - 1000 * 60 * 60 * 24);

        HashSet set = FileListUtil.filesList("/data");

        for (Object o : set) {
            hashSet.add(StringUtils.substringBeforeLast(o.toString(), "/"));
        }

        for (Object o : hashSet) {
            String dirpath = o.toString() + "/" + StringUtils.substringBeforeLast(date, "-") + "/" + date;
            //String dirpath = "/data2/dcx/dhlk_tenant_207333/2021-01/2021-01-08";
            //String dirpath = "/data2/dhlk_tenant_232172/dhlk_tb_product_213407/2021-01/2021-01-08";
            System.out.println("dirpath      " + dirpath);

            Configuration conf = new Configuration();

            conf.set("fs.defaultFS", "hdfs://ip-172-31-16-9.cn-northwest-1.compute.internal:8020");
            String pathStr = "";
            String s = "";
            try {
                // 获取文件系统
                FileStatus[] files = getFileStatuses(dirpath, conf);
                if (files == null || files.length == 0) {
                    throw new FileNotFoundException("Cannot access " + dirpath + ": No such file or directory.");
                } else {
                    dirpath = StringUtils.substringBeforeLast(dirpath, "/");
                    files = getFileStatuses(dirpath, conf);
                }
                int i = 0;
                // /data2/dhlk_tenant_685000/dhlk_tb_product_300141/2021-01/2021-01-26
                String pathname = dirpath + "/" + new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis());
                if (testListStatus(pathname)) {
                    i = files.length - 2;
                } else {
                    i = files.length - 1;
                }
                pathStr = files[i].getPath().toString();

                System.out.println("pathStr  "+pathStr);

                s = pathStr.substring(58);

                FileSystem fs = files[i].getPath().getFileSystem(conf);

//                    hdfs://nameservice1/data2/dcx/dhlk_tenant_207333/2021-01/2021-01-08/log.1610070946208.txt.tmp
                // 文件大小
                long totalSize_count = fs.getContentSummary(files[i].getPath()).getLength();
                // 该目录下所有的文件的数量
                long totalFileCount = listAll(conf, files[i].getPath());

                System.out.println(("".equals(pathStr) ? "." : pathStr) + "\t" + totalSize_count + "\t" + totalFileCount);
                // 把字节换算成 mb
                long totalSize = totalSize_count / 1024;

                String sql = "";

                sql = "insert into dhlk_hdfs_size(path,tenant,device,total_size,total_count,data_date)" +
                        " values('" + pathStr + "','" + s.split("/")[1] + "','" + s.split("/")[2] + "'," + totalSize + "," + totalFileCount + ",'" +date + "')";
                System.out.println(sql);


                PreparedStatement ptmt = null;
                try {
                    ptmt = conn.prepareStatement(sql);
                    ptmt.executeUpdate();
                } catch (Exception e) {
                    e.printStackTrace();

                } finally {
                    //conn.close();
                    try {
                        fs.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            } catch (Exception e) {
                //获取当前系统时间
                // 如果有FileNotFoundException异常，表示没有当天的数据产生，当天的数据量为0
                String sql = "";
                sql = "insert into dhlk_hdfs_size(path,tenant,device,total_size,total_count,data_date)" +
                        " values('" + dirpath + "','" + dirpath.split("/")[2] + "','" + dirpath.split("/")[3] + "',0,0,'" + date + "')";
                System.out.println(sql);
                try {
                    PreparedStatement ptmt = conn.prepareStatement(sql);
                    ptmt.executeUpdate();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                e.printStackTrace();
            }

        }


    }

    private static FileStatus[] getFileStatuses(String dirpath, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(dirpath);
        // 获取文件列表
        return fileSystem.listStatus(path);
    }

    /**
     * 遍历目录下所有文件
     *
     * @param conf 配置
     * @param path 目录
     * @return 该目录下文件数量
     * @throws IOException
     */
    public static Long listAll(Configuration conf, Path path) throws IOException {
        long totalFileCount = 0;
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(path)) {
            FileStatus[] stats = fs.listStatus(path);
            for (int i = 0; i < stats.length; ++i) {
                if (!stats[i].isDir()) {
                    // regular file
                    // System.out.println(stats[i].getPath().toString());
                    totalFileCount++;
                } else {
                    // dir
                    // System.out.println(stats[i].getPath().toString());
                    totalFileCount += listAll(conf, stats[i].getPath());
                }
            }
        }
        fs.close();

        return totalFileCount;
    }


    public static boolean testListStatus(String path) {

        boolean b = true;
        FileSystem fs = null;
        try {

            // 1 获取文件配置信息
            Configuration configuration = new Configuration();
             fs = FileSystem.get(new URI("hdfs://ip-172-31-16-9.cn-northwest-1.compute.internal:8020/"), configuration, "root");
            // 2 判断是文件还是文件夹
            FileStatus[] listStatus = new FileStatus[0];
            fs.listStatus(new Path(path));


        } catch (Exception e) {
            e.printStackTrace();
            b = false;
        }finally {
            // 3 关闭资源
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return b;

    }


}
