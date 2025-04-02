package com.prac.kafka.kf_consumer_service.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prac.kafka.kf_consumer_service.entity.Passenger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.data.mapping.MappingException;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
public class PassengerSerializer implements Serializer<Passenger> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String s, Passenger passenger) {
        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.findAndRegisterModules();
        try {
            if(passenger == null) {
                log.error("Null object received for serializing");
                return null;
            }

            return objectMapper.writeValueAsBytes(passenger);
        }catch (Exception e){
            log.error("Error serializing object");
            throw new MappingException("Exception occurred during serializing ");
        }
    }


    @Override
    public void close() {
    }
}
