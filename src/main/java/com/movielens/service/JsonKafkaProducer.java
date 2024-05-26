package com.movielens.service;

import com.movielens.entity.Review;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaProducer.class);

    private static KafkaTemplate<String, Review> kafkaTemplate;

    public JsonKafkaProducer(KafkaTemplate<String, Review> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public static <E> void sendMessage(String topic, E data) {
        LOGGER.info("Message sent: " + data.toString());
        Message<E> message = MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build();

        kafkaTemplate.send(message);
    }
}
