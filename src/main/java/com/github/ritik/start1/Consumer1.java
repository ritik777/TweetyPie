package com.github.ritik.start1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer1 {
    public static void main(String[] args) {


        final Logger logger = LoggerFactory.getLogger(Consumer1.class.getName());
        String bootstrapServer = "127.0.0.1:9092";
        String groupId= "second-class";
        //BasicConfigurator.configure();
        //create consumer configurations
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //subscribe consumer to our topics
        consumer.subscribe(Collections.singletonList("first_topic"));

        //poll for new data
          while(true) {
              ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

              for (ConsumerRecord<String, String> record : records) {
                  logger.info("Key" + record.key() + "value" + record.value() +
                          "Partition" + record.partition() + "Offset" + record.offset());

              }
          }


    }
}
