package com.example.springkafkaproducerconsumer.config;

import com.example.springkafkaproducerconsumer.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaProducerConfig {

    public Producer<String, Student> getProducerConfig(){
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"my-student");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        Producer<String, Student> producer = new KafkaProducer<>(props);
        return producer;

    }
}
