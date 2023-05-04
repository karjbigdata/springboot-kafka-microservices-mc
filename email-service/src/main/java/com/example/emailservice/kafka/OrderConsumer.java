package com.example.emailservice.kafka;

import com.example.basedomains.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = "${spring.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeData(OrderEvent orderEvent){
        LOGGER.info(String.format("Order event Received in Email service is %s", orderEvent.toString()));

        // save the order event data into the database
    }

}
