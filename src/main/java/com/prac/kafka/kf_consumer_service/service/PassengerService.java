package com.prac.kafka.kf_consumer_service.service;


import com.prac.kafka.kf_consumer_service.entity.KafkaWrapper;
import com.prac.kafka.kf_consumer_service.entity.Passenger;
import com.prac.kafka.kf_consumer_service.repo.PassengerRepo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class PassengerService {

    @Value("${service.kafka.consumer.topic}")
    private String topic;

    @Autowired
    KafkaTemplate<String,Passenger> passengerKafkaTemplate;

    @Autowired
    PassengerRepo repo;

    @Autowired
    BatchProcessor processor;

    public boolean publishPassenger(Passenger passenger){

        CompletableFuture<SendResult<String,Passenger>> future;
        log.info("topic {}",topic);
        try {
            MessageBuilder<Passenger> passengerMessageBuilder = MessageBuilder.withPayload(passenger)
                    .setHeader(KafkaHeaders.TOPIC, topic);

            future = passengerKafkaTemplate.send(passengerMessageBuilder.build());

            passengerKafkaTemplate.flush();

            future.whenComplete(((stringPassengerSendResult, ex) -> {
                if (ex == null)
                    log.info("Message published successfully");
                else
                    log.error("Unable to send message ", ex);
            }));
        }catch (Exception e){
            log.error("Failed to send message ", e);
        }
        return true;
    }


    public void savePassenger(KafkaWrapper wrapper){
        repo.savePassenger(wrapper).subscribe();
    }


    public void savePassengerInBatches(KafkaWrapper wrapper){
        processor.process(wrapper);
    }


}
