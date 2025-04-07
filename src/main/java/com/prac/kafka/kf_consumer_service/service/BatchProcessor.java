package com.prac.kafka.kf_consumer_service.service;


import com.prac.kafka.kf_consumer_service.entity.KafkaWrapper;
import com.prac.kafka.kf_consumer_service.repo.PassengerRepo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class BatchProcessor {


    private final Queue<KafkaWrapper> buffer = new ConcurrentLinkedQueue<>();

    private final Duration flushInterval = Duration.ofSeconds(5);

    @Value("${service.upsert.batch.size}")
    private int batchSizeCapacity;

    @Autowired
    PassengerRepo repo;

    public BatchProcessor(){
        startTimeFlush();
    }

    public void process(KafkaWrapper wrapper){

        buffer.add(wrapper);
        if(buffer.size() >= batchSizeCapacity)
            flush();

    }

    private void flush(){
        List<KafkaWrapper> batch = new ArrayList<>();
        while (!buffer.isEmpty() && batch.size() < batchSizeCapacity)
            batch.add(buffer.poll());
        if(!batch.isEmpty()){
            repo.insertPassengers(batch)
                    .doOnSuccess(v->log.info("Batch update successful"))
                    .doOnError(x -> log.error("failed to insert batch to db"))
                    .subscribe();
        }
    }


    private void startTimeFlush(){
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::flush
                        ,flushInterval.toSeconds()
                        ,flushInterval.toSeconds()
                        , TimeUnit.SECONDS);
    }

}
