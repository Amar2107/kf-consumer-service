package com.prac.kafka.kf_consumer_service.service;


import com.prac.kafka.kf_consumer_service.entity.KafkaWrapper;
import com.prac.kafka.kf_consumer_service.entity.Passenger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PassengerListenerService {

    @Autowired
    PassengerService service;

    @Value("${service.enable.batch.saves}")
    private boolean batchEnabled;

    @KafkaListener(topics = "${service.kafka.consumer.topic}", groupId = "${service.kafka.consumer.group}", containerFactory = "passengerListenerFactory")
    public void listener(ConsumerRecord<String,Passenger> consumerRecord, Acknowledgment acknowledgment){
        try {
            Passenger passenger = consumerRecord.value();

            if (passenger == null)
                log.error("Null object sent");
            else {
                log.info("Object recieved {}", passenger);
                KafkaWrapper wrapper =new KafkaWrapper(passenger, acknowledgment);
                if(batchEnabled)
                    service.savePassengerInBatches(wrapper);
                else
                    service.savePassenger(wrapper);
            }
        }catch (Exception e){
            log.error("Exception occured while consuming message ", e);
        }
    }

}
