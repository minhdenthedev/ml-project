/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.config;

import java.util.Properties;

/**
 *
 * @author minh
 */
public class KafkaProducerConfig extends Properties { 
    
    public KafkaProducerConfig() {
        put("bootstrap.servers", "localhost:9092");
        put("linger.ms", 1);
        put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }
    
}
