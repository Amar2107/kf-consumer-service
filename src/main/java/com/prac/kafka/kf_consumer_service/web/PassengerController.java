package com.prac.kafka.kf_consumer_service.web;

import com.prac.kafka.kf_consumer_service.entity.Passenger;
import com.prac.kafka.kf_consumer_service.service.PassengerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class PassengerController {

    @Autowired
    PassengerService service;

    @PostMapping("/publishPassenger")
    public Object publishPassenger(@RequestBody Passenger passenger){
        return service.publishPassenger(passenger)?ResponseEntity.status(HttpStatus.OK): ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR);
    }

}
