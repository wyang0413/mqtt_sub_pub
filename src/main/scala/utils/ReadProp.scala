package utils

import java.util.ResourceBundle

/**
 * Created with IntelliJ IDEA
 *
 * @Auther :yangwang
 *         Data:2020/7/21
 *         Time:9:03
 * @Description:
 */
object ReadProp {

  /**
   * 读取配置文件
   * @param fileName  配置文件名称
   * @param key 配置key
   * @return
   */
  def getValuefromProperties(fileName: String, key: String): String = {
    val bundle = ResourceBundle.getBundle(fileName)
    val str: String = bundle.getString(key)
    str
  }
}
