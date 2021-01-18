package monitor.mqtt;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.text.SimpleDateFormat;

/**
 * Created with IntelliJ IDEA
 *
 * @Auther :yangwang
 * Data:2020/7/30
 * Time:12:04
 * @Description:
 */
public class Test {
    /**
     * 启动入口
     *
     * @param args
     * @throws MqttException
     */
    public static void main(String[] args) throws MqttException, InterruptedException {

        MqttServer server = new MqttServer();
        server.message = new MqttMessage();

        server.message.setQos(1);  //保证消息能到达一次
        server.message.setRetained(true);


        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String format1 = format.format(System.currentTimeMillis());

//        String data = "{\"table\":\"dhlk_tb_product_902373\",\"factoryCode\":\"dhlk_tenant_232172\",\"after\":{\"energy_ts\":1600334211000,\"energy_id\":3,\"t_act_energy\":0,\"t_react_energy\":0,\"t_act_pwr\":0,\"t_react_pwr\":0,\"t_act_energy_A\":0,\"t_react_energy_A\":0,\"E_V_A\":0,\"E_I_A\":0,\"act_pwr_A\":0,\"react_pwr_A\":0,\"pwr_rate_A\":0,\"t_act_energy_B\":0,\"t_react_energy_B\":0,\"E_V_B\":0,\"E_I_B\":0,\"act_pwr_B\":0,\"react_pwr_B\":0,\"pwr_rate_B\":0,\"t_act_energy_C\":0,\"t_react_energy_C\":0,\"E_V_C\":0,\"E_I_C\":0,\"act_pwr_C\":0,\"react_pwr_C\":0,\"pwr_rate_C\":0,\"freq\":0,\"deviceCode\":\"dhlk_tb_product_902373\"}}";

        String data = "{\"table\":\"dhlk_tb_product_976865\",\"factoryCode\":\"dhlk_tenant_813305\",\"after\":{\"tmOnlineState\":1,\"deviceId\":\"dhlk_tb_product_976865\",\"create_time\":1608840087066}}";
        int count = 0;
        while (true) {
            server.message.setPayload((data).getBytes());
            server.publish(server.topic11, server.message);
            Thread.sleep(1000);
        }

    }
}
