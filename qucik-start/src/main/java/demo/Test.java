package demo;

import org.apache.rocketmq.client.exception.MQClientException;

public class Test {
    public static void main(String[] args) throws MQClientException {
        Thread consumerThread = new Thread(new DemoConsumer());
        Thread producerThread = new Thread(new DemoProducer());

        consumerThread.start();
        producerThread.start();
    }
}
