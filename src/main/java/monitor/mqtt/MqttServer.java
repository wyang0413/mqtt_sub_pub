package monitor.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Created with IntelliJ IDEA
 *
 * @Auther :yangwang
 * Data:2020/7/30
 * Time:11:39
 * @Description: mosquitto的服务端
 */
public class MqttServer {

    public static final String HOST = "tcp://mq.dahanglink.com:1883";

    public static final String TOPIC = "fineworld";

    //定义MQTT的ID，可以在MQTT服务配置中指定
    private static final String clientid = "server";

    MqttClient client;
    MqttTopic topic11;

    // 用户名和密码
    private String userName = "dhlk";  //非必须
    private String passWord = "dhlktech";  //非必须

    MqttMessage message;

    /**
     * 构造函数
     *
     * @throws MqttException
     */
    public MqttServer() throws MqttException {
        // MemoryPersistence设置clientid的保存形式，默认为以内存保存
        client = new MqttClient(HOST, clientid, new MemoryPersistence());
        connect();
    }

    /**
     * 用来连接服务器的方法
     */
    private void connect() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);
        options.setUserName(userName);
        options.setPassword(passWord.toCharArray());
        // 设置超时时间
        options.setConnectionTimeout(10);
        // 设置会话心跳时间
        options.setKeepAliveInterval(20);
        try {
            client.setCallback(new PushCallback());
            client.connect(options);

            topic11 = client.getTopic(TOPIC);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * publish方法
     *
     * @param topic
     * @param message
     * @throws MqttPersistenceException
     * @throws MqttException
     */
    public void publish(MqttTopic topic, MqttMessage message) throws MqttPersistenceException, MqttException {
        MqttDeliveryToken token = topic.publish(message);
        token.waitForCompletion();
        System.out.println("message is published completely! " + token.isComplete());
    }
}
