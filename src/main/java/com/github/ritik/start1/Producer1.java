package com.github.ritik.start1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;

public class Producer1 {
    public static void main(String[] args) {


//Setting producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
//producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String,String> record = new ProducerRecord<String, String>( "first_topic", "Hello World");

//        producer.send(record, new Callback() {
//            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                if(e!= null){
//
//                }
//            }
//        })
        producer.flush();
        producer.close();
    }

}
