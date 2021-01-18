package monitor

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util.Date

import monitor.hdfs.DBUtil
import redis.clients.jedis.Jedis

/**
 * Created with IntelliJ IDEA
 *
 * @Auther :yangwang
 *         Data:2021/1/11
 *         Time:10:49
 * @Description:
 */
object ReadRedis {
  private val conn: Connection = DBUtil.getConnection

  def main(args: Array[String]): Unit = {

    val array = getDevice // 获取所有的设备 list

    // 每个设备开启一个线程
    array.foreach(a => {

      new Thread(new Runnable {
        override def run(): Unit = {
          // 查询设备状态
          getStateOfDevice(a)
        }
      }).start()

    })

  }

  /**
   * 通过key查询redis，如果为空该设备下线
   *
   * @param k
   */
  def getStateOfDevice(k: String) = {
    val jedis = new Jedis("192.168.2.162", 6379, 300000)
    var str: String = ""
    var count: Int = 0
    var b: Boolean = false
    var tu: (String, String, String) = (new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(System.currentTimeMillis())), "", "")
    while (true) {

      val v = jedis.get(k)

      // 判断设备的key为null，该key过期，说明设备下线
      if (v == null && !b) {
        tu = (new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(System.currentTimeMillis())), k, "下线")
        val sql = "insert into device_online_state values('" + tu._1 + "','" + tu._2 + "','" + tu._3 + "')"
        val statement: PreparedStatement = conn.prepareStatement(sql)
        statement.execute()
        b = true

      } else if (v != null && v.equals(str)) {
        count = count + 1
        println(count)
        if (count >= 10) {
          println("下线 : " + count)
          val tuple: (String, String, String) = (new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(System.currentTimeMillis())), k, "下线")
          if (tuple._3.equals(tu._3)) {
            println("========")
          } else {
            println("插入数据库")
            val sql = "insert into device_online_state values('" + tuple._1 + "','" + tuple._2 + "','" + tuple._3 + "')"
            val statement: PreparedStatement = conn.prepareStatement(sql)
            statement.execute()
            tu = tuple
          }
          count = 0
        }
      } else if (v != null) {
        str = v
        println(str)
        println("上线啦 : ")
        val tuple: (String, String, String) = (new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(System.currentTimeMillis())), k, "上线")
        if (tuple._3.equals(tu._3)) {
          println("========")
        } else {
          val sql = "insert into device_online_state values('" + tuple._1 + "','" + tuple._2 + "','" + tuple._3 + "')"
          val statement: PreparedStatement = conn.prepareStatement(sql)
          statement.execute()
          println("插入数据库")
          tu = tuple
        }
        count = 0
      }
      Thread.sleep(30000)
    }

  }

  /**
   * 查询数据库左右的租户和设备
   *
   * @return
   */
  def getDevice = {
    val sql = "select DISTINCT(device),tenant from dhlk_hdfs_size"
    val statement: Statement = conn.createStatement()
    val set: ResultSet = statement.executeQuery(sql)
    var str: String = ""
    var list = List[String]()

    while (set.next()) {
      val tenant: String = set.getString("tenant") // 租户
      val device: String = set.getString("device") // 设备
      str = tenant + ":" + device
      list = str +: list
    }
    list
  }


}
