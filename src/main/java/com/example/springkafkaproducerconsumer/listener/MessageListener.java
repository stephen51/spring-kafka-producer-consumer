package com.example.springkafkaproducerconsumer.listener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
public class MessageListener implements ApplicationListener<ApplicationReadyEvent> {

    @Autowired
    MessageReceiver messageReceiver;
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        Thread prodThread = new Thread((Runnable) messageReceiver, MessageReceiver.class.getSimpleName());
        prodThread.start();

    }
}
