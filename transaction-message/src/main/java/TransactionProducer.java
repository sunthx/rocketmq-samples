import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        TransactionListener transactionListener = new TransactionListenerImpl();
        TransactionMQProducer transactionMQProducer = new TransactionMQProducer(
               "transactionMQProducer_group"
        );

        ExecutorService executorService = new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("client-transaction-msg-check-thread");
                        return thread;
                    }
                }
        );

        transactionMQProducer.setNamesrvAddr("192.168.2.71:9876");
        transactionMQProducer.setExecutorService(executorService);
        transactionMQProducer.setTransactionListener(transactionListener);
        transactionMQProducer.start();

        String[] tags = new String[]{"TagA","TagB","TagC","TagD","TagE"};
        for (int i = 0; i < 10; i++){
            try {
                Message msg = new Message(
                        "transaction_message_topic_test",
                        tags[i % tags.length],
                        "KEY" + i,
                        ("HELLO RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
                );

                SendResult sendResult = transactionMQProducer.sendMessageInTransaction(msg,null);
                System.out.printf("%s%n",sendResult);
                Thread.sleep(10);
            } catch (UnsupportedEncodingException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        Thread.sleep(1000 * 10000);
        transactionMQProducer.shutdown();
    }
}
