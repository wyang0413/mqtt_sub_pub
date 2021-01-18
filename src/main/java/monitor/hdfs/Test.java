package monitor.hdfs;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * Created with IntelliJ IDEA
 *
 * @Auther :yangwang
 * Data:2021/1/12
 * Time:9:32
 * @Description:
 */
public class Test {
    public static void main(String[] args) {
        String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getDate()-1);
        System.out.println(date);
    }
}
