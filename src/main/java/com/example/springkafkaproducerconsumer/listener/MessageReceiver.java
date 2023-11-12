package com.example.springkafkaproducerconsumer.listener;

import com.example.springkafkaproducerconsumer.config.KafkaConsumerConfig;
import com.example.springkafkaproducerconsumer.consumer.KafkaConsumer;
import com.example.springkafkaproducerconsumer.producer.KafkaProducer;
import com.example.springkafkaproducerconsumer.service.BusinessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class MessageReceiver implements Runnable {


    private static final Logger log = LoggerFactory.getLogger(MessageReceiver.class);

    @Autowired
    KafkaConsumerConfig kafkaConsumerConfig;

    public static final String TOPIC = "student-topic";

    @Autowired
    BusinessService businessService;


    @Override
    public void run() {
        log.info(Thread.currentThread().getName()+"Started.");
        try{
            ExecutorService executorService = Executors.newFixedThreadPool(2);
            executorService.execute(new KafkaConsumer(kafkaConsumerConfig.getConsumerConfig(), TOPIC, 5, businessService ));
            while (true){
                if(!KafkaConsumer.isInitialLoadCompleted){
                    Thread.sleep(30 * 1000);
                }else{
                    log.info("Consumed message from the topic:{} ",TOPIC);
                    break;
                }

            }
        }catch (Exception e){
            log.info(e.getMessage());
        }

    }
}
