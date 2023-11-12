package com.example.springkafkaproducerconsumer.controller;

import com.example.springkafkaproducerconsumer.model.Student;
import com.example.springkafkaproducerconsumer.producer.KafkaProducer;
import com.example.springkafkaproducerconsumer.service.BusinessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

   @Autowired
   KafkaProducer kafkaProducer;

   @Autowired
   BusinessService businessService;

    @PostMapping("/api/produce")
    public String produceMessage(@RequestBody Student message){
        kafkaProducer.send(message);
        return "Message Published";
    }

    @GetMapping("/api/applypause")
    public String applyPause() throws InterruptedException {
        businessService.applyPause();
        Thread.sleep(15000);
        businessService.removePause();
        return "Pause Triggered";
    }
}
