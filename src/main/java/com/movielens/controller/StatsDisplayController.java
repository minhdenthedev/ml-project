package com.movielens.controller;

import com.movielens.config.KafkaConsumerConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.io.*;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author minh
 */
@RestController
public class StatsDisplayController {

    final private static Consumer<String, String> consumer = new KafkaConsumer<>(new KafkaConsumerConfig());

    private static Map<String, String> buffer = new HashMap<>();

    @GetMapping("/stats")
    public ResponseEntity<Map<String, String>> getStats(Model model) {
        consumer.subscribe(Arrays.asList("rated-events"));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

        for (ConsumerRecord<String, String> record : records) {
            String value = record.value() + " at " + record.timestamp() + ", offset=" + record.offset();
            buffer.put(record.key(), value);
        }
        System.out.println(buffer);
        return new ResponseEntity<>(buffer, HttpStatus.OK);
    }

    @GetMapping("/top-avg-rating")
    public ResponseEntity<Map<String, List<String>>> getTopRating(Model model) {
        Map<String, List<String>> result = new HashMap();
        try {
            BufferedReader br = new BufferedReader(new FileReader("/home/minh/Work/ml-project/hive_results/avg_rating/000000_0")); 
            String line = "";  
            String splitBy = "\t";  
            List<String> titles = new ArrayList<>();
            List<String> rates = new ArrayList<>();

            while ((line = br.readLine()) != null) {
                String[] fields = line.split(splitBy);    // use comma as separator 
                titles.add(fields[1]);
                rates.add(fields[3]);
            }

            result.put("titles", titles);
            result.put("rates", rates);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }
}
