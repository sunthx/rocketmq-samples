package demo;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

public class DemoProducer implements Runnable {
    public void run() {
        DefaultMQProducer producer = new DefaultMQProducer("producer");
        producer.setNamesrvAddr("192.168.2.100:9876");

        try {
            producer.start();

            for (int i = 0; i < 100; i++) {
                Message msg = new Message("TopicTest","TagA",("Hello RocketMQ" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n",sendResult);
            }

            producer.shutdown();
        } catch (MQClientException | RemotingException | InterruptedException | UnsupportedEncodingException | MQBrokerException e) {
            e.printStackTrace();
        }
    }
}
