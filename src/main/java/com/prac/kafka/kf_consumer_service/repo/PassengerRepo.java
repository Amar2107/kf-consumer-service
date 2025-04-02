package com.prac.kafka.kf_consumer_service.repo;

import com.prac.kafka.kf_consumer_service.entity.Passenger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;


@Slf4j
@Service
public class PassengerRepo {

    @Qualifier("passengerMongoTemplate")
    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;


    public Mono<Passenger> savePassenger(Passenger passenger){

        Mono<Passenger> passengerMono = null;
        try {
           passengerMono =  reactiveMongoTemplate.save(passenger)
                   .doOnSuccess(savedPass -> {log.info("Passenger saved successfully {}",savedPass);})
                   .doOnError(error -> log.error("Error occurred while saving ",error));

        }catch (Exception e){
            log.error("Error saving passenger");
        }
        return passengerMono;
    }


}
