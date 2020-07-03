package com.github.ritik.start1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer1CallBack {
    public static void main(String[] args) {
        BasicConfigurator.configure();
        final Logger logger = LoggerFactory.getLogger(Producer1CallBack.class);
//Setting producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
//producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String,String> record = new ProducerRecord<String, String>( "first_topic", "Hello World");

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes everytime when a record is sent successfully or exception is thrown
                if(e== null){
                    //sent successfully
                    logger.info("Recieved new Meta data. \n" +
                            "Topic" + recordMetadata.topic() + "\n" +
                            "Partition" + recordMetadata.partition() + "\n" +
                            "Offset" + recordMetadata.offset() + "\n" +
                            "Timestamp" + recordMetadata.timestamp() );

                }
                else{
                    logger.error("error while producing",e);
                }

            }
        });
        producer.flush();
        producer.close();
    }

}
