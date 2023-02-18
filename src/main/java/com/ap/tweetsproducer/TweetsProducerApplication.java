package com.ap.tweetsproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class TweetsProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(TweetsProducerApplication.class, args);
    }

}
