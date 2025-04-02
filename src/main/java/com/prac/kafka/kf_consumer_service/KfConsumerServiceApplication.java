package com.prac.kafka.kf_consumer_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class KfConsumerServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(KfConsumerServiceApplication.class, args);
	}

}
