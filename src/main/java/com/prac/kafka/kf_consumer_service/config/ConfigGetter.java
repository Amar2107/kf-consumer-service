package com.prac.kafka.kf_consumer_service.config;

import lombok.Data;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


@Data
@Component
public class ConfigGetter {

    @Value("${service.kafka.server}")
     private String bootStrap;

    @Value("${service.kafka.consumer.group}")
    private String groupId;

    @Value("${service.kafka.consumer.concurrency}")
    private Integer consumerConcurrency;

}
