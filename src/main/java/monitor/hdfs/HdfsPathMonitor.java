package monitor.hdfs;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * hdfs具体的目录文件所占内存大小
 */
public class HdfsPathMonitor {
    // submit shell
    /*
     * main类的路径不需要指定，否则会被认为是参数传递进入。
     * yarn jar /app/m_user1/service/Hangzhou_HdfsFileMananger.jar /hive_tenant_account/hivedbname/
     */

    public static void main(String[] args) {
//        System.out.println("the args is " + String.join(",", args));
        Connection conn = DBUtil.getConnection();
        //获取当前系统时间
        String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        // 需要监控的目录
        HashSet hashSet = FileListUtil.filesList("/data2");
//        HashSet filesList = FileListUtil.filesList("/dhlk_lighting");


        String dt = "2021-1-8";

        for (Object o : hashSet) {

            //String dirPath = "/dhlk/dhlk_lighting/dhlk_tenant_243237";
            String dirPath = o.toString();
            System.out.println("dirpath      " + dirPath);

            Configuration conf = new Configuration();
            // 在配置文件里引入core-site.xml，hdfs-site.xml，yarn-site.xml
            conf.set("fs.defaultFS", "hdfs://nameservice1");
            String pathStr = "";
            String s = "";
            try {
                // 获取文件系统
                FileSystem fileSystem = FileSystem.get(conf);
                Path path = new Path(dirPath);
                // 获取文件列表
                FileStatus[] files = fileSystem.listStatus(path);

                System.out.println("dirpath \t total file size \t total file count");
                for (int i = 0; i < files.length; i++) {
                     pathStr = files[i].getPath().toString();

                     s = pathStr.substring(20);

                    FileSystem fs = files[i].getPath().getFileSystem(conf);
                    // 文件大小
                    long totalSize_count = fs.getContentSummary(files[i].getPath()).getLength();
                    // 该目录下所有的文件的数量
                    long totalFileCount = listAll(conf, files[i].getPath());

                    System.out.println(("".equals(pathStr) ? "." : pathStr) + "\t" + totalSize_count + "\t" + totalFileCount);
                    // 把字节换算成 Mb
                    long totalSize = totalSize_count ;
                    int index = pathStr.lastIndexOf("/");
                    String sql = "";

                    //             hdfs://nameservice1/data2/dcx/dhlk_tenant_207333/2020-10/2020-10-01/fsdfhgfdh   hdfs://nameservice1/data2/dcx/dhlk_tenant_207333/2020-12/2020-12-01        data2/dcx/dhlk_tenant_207333/2020-10/2020-10-01

                    if (dirPath.contains("dcx")) {
                        sql = "insert into dhlk_memory_size(shift_date,path,tenant,device,total_size,total_count,data_date)" +
                                " values('" + date + "', '" + pathStr + "','" + s.split("/")[2] + "','" + s.split("/")[1] + "'," + totalSize + "," + totalFileCount + ",'" + StringUtils.substringAfterLast(s,"/")+ "')";
                        System.out.println(sql);
                    } else {

                        sql = "insert into dhlk_memory_size(shift_date,path,tenant,device,total_size,total_count,data_date)" +
                                " values('" + date + "', '" + pathStr + "','" + s.split("/")[1] + "','" + s.split("/")[2] + "'," + totalSize + "," + totalFileCount + ",'" + StringUtils.substringAfterLast(s,"/")+ "')";
                        System.out.println(sql);
                    }

                    PreparedStatement ptmt = null;
                    try {
                        ptmt = conn.prepareStatement(sql);
                        ptmt.executeUpdate();
                    } catch (SQLException e) {
                        e.printStackTrace();

                    } finally {
                        //conn.close();
                        try {
                            fs.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                }
            } catch (Exception e) {
                //获取当前系统时间
                // 如果有FileNotFoundException异常，表示没有当天的数据产生，当天的数据量为0
                String sql = "";
                if (dirPath.contains("dcx")) {
                    sql = "insert into dhlk_memory_size(shift_date,path,tenant,device,total_size,total_count,data_date)" +
                            " values('" + date + "', '" + pathStr + "','" + s.split("/")[2] + "','" + s.split("/")[1] + "',0,0,'" + StringUtils.substringAfterLast(s,"/")+ "')";
                    System.out.println(sql);
                } else {

                    sql = "insert into dhlk_memory_size(shift_date,path,tenant,device,total_size,total_count,data_date)" +
                            " values('" + date + "', '" + pathStr + "','" + s.split("/")[1] + "','" + s.split("/")[2] + "',0,0,'" + StringUtils.substringAfterLast(s,"/")+ "')";
                    System.out.println(sql);
                }
                try {
                    PreparedStatement ptmt = conn.prepareStatement(sql);
                    ptmt.executeUpdate();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
                e.printStackTrace();
            }

        }


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



}