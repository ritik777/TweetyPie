package com.github.ritik.start1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThreads {
    public static void main(String[] args) throws InterruptedException {
        new ConsumerThreads().run();

    }

private ConsumerThreads(){

}
public void run() {
    final Logger logger = LoggerFactory.getLogger(ConsumerThreads.class.getName());
    CountDownLatch latch = new CountDownLatch(1);
    String bootstrapServer = "127.0.0.1:9092";
    String groupId= "third-class";
    String topic = "first_topic";
    Runnable myConsumerThread = new ConsumerThread(bootstrapServer, groupId,topic, latch);

    Thread myThread = new Thread(myConsumerThread);
    myThread.start();

    Runtime.getRuntime().addShutdownHook(new Thread( () ->{
        logger.info("Caught SHutdown Hook!");


        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }));
    try {
        latch.await();
    } catch (InterruptedException e) {
        logger.error("application interruppted!");
    }
    finally{
        logger.info("Application is closing");
    }


}

    public class ConsumerThread implements Runnable{
        private final CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        final Logger logger = LoggerFactory.getLogger(ConsumerThreads.class.getName());


        public ConsumerThread( String bootstrapServer,
                              String groupId,
                              String topic,
                              CountDownLatch latch){
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.latch = latch;
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }


        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {

                        logger.info("Key" + record.key() + "value" + record.value() +
                                "Partition" + record.partition() + "Offset" + record.offset());

                    }
                }
            } catch (WakeupException e){
                logger.info("Received SHutdown Signal!");

            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown(){
            consumer.wakeup();

        }
    }
}
