package com.example.springkafkaproducerconsumer.producer;

import com.example.springkafkaproducerconsumer.config.KafkaProducerConfig;
import com.example.springkafkaproducerconsumer.model.Student;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    public static final String TOPIC = "student-topic";

    @Autowired
    KafkaProducerConfig kafkaProducerConfig;


    Producer<String, Student> producer;

    public void send(Student message){

        try{
            if(producer == null){
                producer = kafkaProducerConfig.getProducerConfig();
            }
            Future<RecordMetadata> recordMetadata = producer.send(new ProducerRecord(TOPIC,String.valueOf(message.getId()),message));
            RecordMetadata recordMetadataValue =recordMetadata.get(10000, TimeUnit.MILLISECONDS);
            log.info("Message Published in the topic:{}, Partition:{}, Offset:{}",recordMetadataValue.topic(),recordMetadataValue.partition(),recordMetadataValue.offset());
        }catch (Exception e){
            log.info("Error Occurred while publishing"+message.getId());
        }

    }


}
