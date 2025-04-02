package com.prac.kafka.kf_consumer_service.serializer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prac.kafka.kf_consumer_service.entity.Passenger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;


import java.nio.charset.StandardCharsets;
import java.util.Map;


@Slf4j
@Component
public class PassengerDeserializer implements Deserializer<Passenger> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Passenger deserialize(String s, byte[] bytes) {
        Passenger passenger = null;
        ObjectMapper objectMapper  = new ObjectMapper();
        try {
            if(bytes != null){
                objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);
                passenger = objectMapper.readValue(new String(bytes, StandardCharsets.UTF_8), Passenger.class);
                return null;
            }

        }catch (Exception e){
            log.error("Error Deserializing passenger object");
            throw new RuntimeException("Exception in Deserializing object");
        }
        return passenger;
    }


    @Override
    public void close() {
    }
}
