package com.prac.kafka.kf_consumer_service.entity;


import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "Passenger")
public class Passenger {

    @Id
    private Long id;
    private String name;
    private String email;
    private String password;
    private String phno;
}
