package com.prac.kafka.kf_consumer_service.service;


import com.prac.kafka.kf_consumer_service.entity.Passenger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PassengerListenerService {

    @Autowired
    PassengerService service;

    @KafkaListener(topics = "${service.kafka.consumer.topic}", groupId = "${service.kafka.consumer.group}", containerFactory = "passengerListenerFactory")
    public void listener(ConsumerRecord<String,Passenger> consumerRecord, Acknowledgment acknowledgment){
        try {
            Passenger passenger = consumerRecord.value();

            if (passenger == null)
                log.error("Null object sent");
            else {
                log.info("Object recieved {}", passenger);
                service.savePassenger(passenger);
            }
        }catch (Exception e){
            log.error("Exception occured while consuming message ", e);
        }finally {
            acknowledgment.acknowledge();
        }
    }

}
