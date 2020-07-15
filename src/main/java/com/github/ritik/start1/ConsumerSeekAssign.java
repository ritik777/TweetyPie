package com.github.ritik.start1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerSeekAssign {
    public static void main(String[] args) {


        final Logger logger = LoggerFactory.getLogger(ConsumerSeekAssign.class.getName());
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        //String groupId= "third-class";
        //BasicConfigurator.configure();
        //create consumer configurations
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

       //assign and seek are mostly used for getting a specific part of data

        //assign
        TopicPartition partition = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partition));

        //seek
        long offset_to_read = 15L;
        consumer.seek(partition,offset_to_read);

        int messages_to_read = 5;
        boolean keep_reading= true;
        int number_of_messages_read_so_far=0;

        //poll for new data
          while(keep_reading) {
              ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

              for (ConsumerRecord<String, String> record : records) {
                  number_of_messages_read_so_far++;
                  logger.info("Key" + record.key() + "value" + record.value() +
                          "Partition" + record.partition() + "Offset" + record.offset());
                  if(number_of_messages_read_so_far>=messages_to_read){
                      keep_reading=false;
                      break;
                  }

              }
          }


    }
}
