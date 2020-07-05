package com.github.ritik.start1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        BasicConfigurator.configure();
        final Logger logger = LoggerFactory.getLogger(ProducerKeys.class);
//Setting producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
//producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++){

            String topic = "first_topic";
            String value = "Hello World";
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String,String> record = new ProducerRecord<String, String>( topic,key,value);
            logger.info("Key" + key);
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
            }).get(); //block the .send() to make it synchronous - not to do in production.
        }


        producer.flush();
        producer.close();
    }

}
