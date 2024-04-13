/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.config;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 *
 * @author minh
 */
public class KafkaConsumerConfig extends Properties{
    public KafkaConsumerConfig() {
        put("bootstrap.servers", "localhost:9092");
        put("group.id", "stats-report");
        put("enable.auto.commit", "true");
        put("auto.commit.interval.ms", "1000");
        put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }
}
