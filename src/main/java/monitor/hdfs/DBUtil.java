package monitor.hdfs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtil
{
    private static final String URL = "jdbc:mysql://192.168.2.162:3306/dhlk_monitor_report?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8&useSSL=true&allowMultiQueries=true";
    private static final String UNAME = "root";
    private static final String PWD = "1111";

    private static Connection conn = null;

    static
    {
        try
        {
            // 1.加载驱动程序
            Class.forName("com.mysql.jdbc.Driver");
            // 2.获得数据库的连接
            conn = DriverManager.getConnection(URL, UNAME, PWD);
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    public static Connection getConnection()
    {
        return conn;
    }
}