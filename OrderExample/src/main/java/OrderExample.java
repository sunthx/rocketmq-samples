import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

public class OrderExample {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order_example");
        producer.setNamesrvAddr("192.168.2.71:9876");
        producer.start();

        String[] tags = new String[] { "TagA" ,"TagB","TagC","TagD","TagE"};
        for (int i = 0; i < 100; i++) {
            int orderId = i % 10;
            Message message = new Message(
                    "OrderMessageTopic",
                    tags[i % tags.length],
                    "KEY" + i,
                    ("Order Message" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            MessageQueueSelector messageQueueSelector = new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Integer id = (Integer) o;
                    int index = id % list.size();
                    return list.get(index);
                }
            };


        }
    }
}
