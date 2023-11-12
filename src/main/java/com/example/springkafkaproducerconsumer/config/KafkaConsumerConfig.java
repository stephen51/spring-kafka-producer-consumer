package com.example.springkafkaproducerconsumer.config;

import com.example.springkafkaproducerconsumer.model.Student;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaConsumerConfig {


    public Consumer<String, Student> getConsumerConfig(){
        Map<String,Object> props  = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-student-id");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.springkafkaproducerconsumer.model");
        return new KafkaConsumer<>(props);
    }
}
