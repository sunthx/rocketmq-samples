import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class OrderExample {
    public static void main(String[] args) throws Exception {
        CustomerTest customerTest = new CustomerTest ();
        ProducerTest producerTest = new ProducerTest();

        Thread customerThread = new Thread(customerTest);
        Thread producerThread = new Thread(producerTest);

        customerThread.start();
        producerThread.start();
    }

    static class CustomerTest implements Runnable{

        public void run() {
            try {
                DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ordered_example_group");
                consumer.setNamesrvAddr("192.168.2.71:9876");
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
                consumer.subscribe("OrderMessageTopic","TagA || TagC || TagD");

                MessageListenerOrderly messageListenerOrderly = new MessageListenerOrderly() {
                    AtomicLong consumeTimes = new AtomicLong(0);
                    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                        consumeOrderlyContext.setAutoCommit(false);
                        System.out.printf(Thread.currentThread().getName() + "Receive New Messages：" + list + "%n");
                        this.consumeTimes.incrementAndGet();

                        if((this.consumeTimes.get() % 2) == 0){
                            return ConsumeOrderlyStatus.SUCCESS;
                        } else if ((this.consumeTimes.get() % 3) == 0) {
                            return ConsumeOrderlyStatus.ROLLBACK;
                        } else if ((this.consumeTimes.get() % 4) == 0) {
                            return ConsumeOrderlyStatus.COMMIT;
                        } else if((this.consumeTimes.get() % 5) == 0){
                            consumeOrderlyContext.setSuspendCurrentQueueTimeMillis(3000);
                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }

                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                };

                consumer.registerMessageListener(messageListenerOrderly);
                consumer.start();
            }
            catch (Exception e){

            }
        }
    }

    static class ProducerTest implements Runnable  {

        public void run()  {

            try {
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

                    SendResult sendResult = producer.send(message,messageQueueSelector,orderId);
                    System.out.printf("%s%n", sendResult);
                }

                producer.shutdown();
            }
            catch (Exception e){

            }
        }
    }
}
