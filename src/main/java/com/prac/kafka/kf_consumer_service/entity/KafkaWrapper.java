package com.prac.kafka.kf_consumer_service.entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.kafka.support.Acknowledgment;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaWrapper {

    private Passenger passenger;
    private Acknowledgment acknowledgment;

}
