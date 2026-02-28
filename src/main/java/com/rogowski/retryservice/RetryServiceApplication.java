package com.rogowski.retryservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class RetryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(RetryServiceApplication.class, args);
    }

}
