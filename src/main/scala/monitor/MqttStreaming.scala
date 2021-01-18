package monitor

import com.alibaba.fastjson.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.mqtt.MQTTUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import redis.clients.jedis.Jedis
import utils.ParseJson

/**
 * Created with IntelliJ IDEA
 *
 * @Auther :yangwang
 *         Data:2021/1/8
 *         Time:14:04
 * @Description:
 */
object MqttStreaming {

  val jedis = new Jedis("192.168.2.162", 6379, 60000)

  def main(args: Array[String]): Unit = {

    val brokerUrl = "tcp://mq.dahanglink.com:1883"
    val array: Array[String] = Array("machines", "dhlk_Energy", "fineworld")
    val username: String = "dhlk"
    val password: String = "dhlktech"
    val conf: SparkConf = new SparkConf().setAppName("mqtt")

    val ssc = new StreamingContext(conf, Milliseconds(100))

    ssc.sparkContext.setLogLevel("WARN")
    array.foreach(topic => {
      val stream_data = MQTTUtils.createStream(ssc, brokerUrl, topic, StorageLevel.MEMORY_ONLY, Some(topic), Some(username), Some(password), None, None, None, None, None)

      val value: DStream[(String, String, AnyRef)] = stream_data
        .filter(x => {
          x.contains("after") && x.contains("table") && x.contains("factoryCode")
        })
        .map(x => {

          val dcx: JSONObject = ParseJson.getJsonData(x)
          val dcxAfter: JSONObject = dcx.getJSONObject("after")
          val factoryCode = dcx.get("factoryCode").toString
          val table = dcx.get("table").toString
          //      val create_time: AnyRef = dcxAfter.get("create_time").toString
          (factoryCode, table, dcxAfter.toString)

        })
      //    value.print()

      value.foreachRDD(part => {

        part.foreach(item => {
          println(item._3.toString)
          jedis.setex(item._1 + ":"+item._2.toString,300,item._3.toString)
        })

      })
    })


    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
