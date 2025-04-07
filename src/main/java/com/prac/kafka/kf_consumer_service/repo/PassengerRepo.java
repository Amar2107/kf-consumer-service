package com.prac.kafka.kf_consumer_service.repo;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.prac.kafka.kf_consumer_service.entity.KafkaWrapper;
import com.prac.kafka.kf_consumer_service.entity.Passenger;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.ReactiveBulkOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;


@Slf4j
@Service
public class PassengerRepo {

    @Qualifier("passengerMongoTemplate")
    @Autowired
    ReactiveMongoTemplate reactiveMongoTemplate;


    public Mono<Passenger> savePassenger(KafkaWrapper wrapper){

        Mono<Passenger> passengerMono = null;
        try {
           passengerMono =  reactiveMongoTemplate.save(wrapper.getPassenger())
                   .doOnSuccess(savedPass -> {
                       log.info("Passenger saved successfully {}",savedPass);
                       wrapper.getAcknowledgment().acknowledge();
                   })
                   .doOnError(error -> log.error("Error occurred while saving ",error));

        }catch (Exception e){
            log.error("Error saving passenger");
        }
        return passengerMono;
    }


    public Mono<Void> insertPassengers(List<KafkaWrapper> wrappers){
        ReactiveBulkOperations ops = reactiveMongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, Passenger.class);

        wrappers.forEach(wrapper -> {
            Query query = Query.query(Criteria.where("_id").is(wrapper.getPassenger().getId()));
            Document document = new Document();
            reactiveMongoTemplate.getConverter().write(wrapper.getPassenger(),document);
            ops.upsert(query, Update.fromDocument(new Document("$set",document)));
        });

        return ops.execute()
                .doOnSuccess(res->{
                    wrappers.forEach(wrapper -> {
                        try {
                            wrapper.getAcknowledgment().acknowledge();
                        }catch (Exception e){log.error("Error acknowledging record {}",wrapper.getPassenger(),e);}
                    });
                })
                .doOnError(e -> log.error("Error upsert failed ",e))
                .then();
    }

}
