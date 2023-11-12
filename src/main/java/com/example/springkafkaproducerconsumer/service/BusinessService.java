package com.example.springkafkaproducerconsumer.service;

import org.springframework.stereotype.Component;

@Component
public class BusinessService {
    public static boolean pauseEnabled = false;

    public boolean isPauseEnabled(){
        return pauseEnabled;
    }

    public void applyPause(){
         pauseEnabled = true;
    }

    public void removePause(){
        pauseEnabled = false;
    }
}
