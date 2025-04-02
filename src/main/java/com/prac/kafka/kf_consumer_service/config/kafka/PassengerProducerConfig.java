package com.prac.kafka.kf_consumer_service.config.kafka;


import com.prac.kafka.kf_consumer_service.config.ConfigGetter;
import com.prac.kafka.kf_consumer_service.entity.Passenger;
import com.prac.kafka.kf_consumer_service.serializer.PassengerSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class PassengerProducerConfig {

    @Autowired
    ConfigGetter config;

    private Map<String, Object> producerConfig(){

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,config.getBootStrap());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PassengerSerializer.class);
        return producerProps;
    }

    @Bean
    public ProducerFactory<String,Passenger> producerFactory(){
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean("PassengerKafkaTemplate")
    public KafkaTemplate<String, Passenger> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }

}
