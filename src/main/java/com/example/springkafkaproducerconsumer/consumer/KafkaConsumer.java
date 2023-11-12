package com.example.springkafkaproducerconsumer.consumer;

import com.example.springkafkaproducerconsumer.model.Student;
import com.example.springkafkaproducerconsumer.service.BusinessService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class KafkaConsumer implements Runnable{

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    Consumer<String, Student> kafkaConsumer;

    private String topic;

    private int retries;

    Map<Integer,Student> studentMap = new HashMap<>();

    public static  boolean isInitialLoadCompleted = false;

    BusinessService businessService;


    public KafkaConsumer(Consumer<String, Student> consumerConfig, String topic, int retries, BusinessService businessService ) {
        this.kafkaConsumer = consumerConfig;
        this.topic = topic;
        this.retries =retries;
        this.businessService = businessService;
    }

    @Override
    public void run() {
        int noDataPoller = 0;

        try(Consumer<String, Student> records = kafkaConsumer) {
            resetOffset(kafkaConsumer);
            int messageReceived = 0;
            boolean isDataAvailable = true;
            int pollCount = 0;

            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
            List<TopicPartition> topicPartitionList = partitionInfos.stream().map(p -> new TopicPartition(topic, p.partition())).collect(Collectors.toList());
            Map<TopicPartition, Long> partitionLastOffset = kafkaConsumer.endOffsets(topicPartitionList);
            Map<TopicPartition, Long> partitionBeginOffset = kafkaConsumer.beginningOffsets(topicPartitionList);
            Map<Integer, Long> lastOffsetMap = new HashMap<>();
            partitionLastOffset.forEach((tp, offset) -> {
                if (!partitionBeginOffset.get(tp).equals(offset)) {
                    lastOffsetMap.put(tp.partition(), offset);
                }
            });
            log.info(" End(+1) offsets for partition: " + lastOffsetMap);
            Map<Integer, Long> fullyReadPartitions = new HashMap<>();
            while (isDataAvailable) {
                if (businessService.isPauseEnabled()) {
                    log.info(" kafka consumer is paused." );
                    kafkaConsumer.pause(kafkaConsumer.assignment());
                } else {
                    kafkaConsumer.resume(kafkaConsumer.assignment());
                    ConsumerRecords<String, Student> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10000));
                    pollCount++;
                    if (consumerRecords != null && !consumerRecords.isEmpty()) {
                        messageReceived += consumerRecords.count();
                        log.info(" Poll-{} : Partition read- {} : message consumed-{} : Total message consumed so far:{} | More Data Available:{}", pollCount, consumerRecords.partitions(), consumerRecords.count(), messageReceived, isDataAvailable);
                        for (ConsumerRecord<String, Student> record : consumerRecords) {
                            if (null != record && null != record.value()) {
                                studentMap.put(record.value().getId(), record.value());
                            }

                            if (record.offset() >= lastOffsetMap.get(record.partition()) - 1) {
                                fullyReadPartitions.put(record.partition(), record.offset());
                                if (fullyReadPartitions.size() == lastOffsetMap.size()) {
                                    isInitialLoadCompleted = true;
                                }
                            }
                        }
                        if (isInitialLoadCompleted) {
                            log.info("{} records are cached", studentMap.size());
                        }

                    } else {
                        if (!isInitialLoadCompleted) {
                            noDataPoller++;

                        }
                    }
                    if (!isInitialLoadCompleted) {
                        isInitialLoadCompleted = noDataPoller > retries;
                    }

                }
            }
        }
        log.info("{} records cached",studentMap.size());

    }

    private void resetOffset(Consumer<String, Student> kafkaConsumer) {
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
        List<TopicPartition> topicPartitionList = partitionInfos.stream().map(p-> new TopicPartition(topic,p.partition())).collect(Collectors.toList());
        kafkaConsumer.assign(topicPartitionList);
        kafkaConsumer.seekToBeginning(topicPartitionList);
    }
}
