package com.speed;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {
    private static final Properties PROPERTIES = new Properties();
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String RATE_TOPIC = "rate-events";
    private static final String VISIT_TOPIC = "visited-events";
    private static final String SEARCH_TOPIC = "search-events";
    private static final String REVIEW_TOPIC = "review-events";

    private static final String GROUP_ID = "consumer-new";
    private static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static KafkaConsumer<String, String> consumer;

    /**
     * TIME_INTERVAL is estimated in nano second, so the below value is equivalent
     * to 10 seconds.
     */
    private static final long TIME_INTERVAL = 1_000_000_000L;

    public static void main(String[] args) {
        /**
         * Output format: id|total|num_occurance|average
         */
        Heap visit_heap = new Heap(CompareStrategy.TOTAL);
        Heap rate_heap = new Heap(CompareStrategy.AVERAGE);
        Map<String, Integer> KEYWORDS = new HashMap<>();
        PriorityQueue<Keyword> PQ = new PriorityQueue<>(Collections.reverseOrder());
        Heap review_heap = new Heap(CompareStrategy.TOTAL);
        createcouconsumer();
        try {
            long checkpoint = System.nanoTime();
            long flush_check_point = System.nanoTime();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String topic = record.topic();
                    System.out.println(topic);
                    switch (topic) {
                        case "visited-events":
                            VisitedEvent.fetchEvents(record, visit_heap);
                            // System.out.println("Visit: " + visit_heap.size());
                            break;
                        case "rate-events":
                            RateEvent.fetchEvents(record, rate_heap);
                            System.out.println("Rate: " + rate_heap.size());
                            break;
                        case "search-events":
                            SearchEvent.fetchEvents(record, KEYWORDS);
                            // System.out.println("Search: " + KEYWORDS.size());
                            break;
                        case "review-events":
                            ReviewEvent.fetchEvents(record, review_heap);
                            // System.out.println("Review: " + review_heap.size());
                            break;
                    }
                }

                // Write files after 3 seconds
                if (System.nanoTime() - checkpoint >= TIME_INTERVAL) {
                    VisitedEvent.writeToFile(visit_heap);
                    RateEvent.writeToFile(rate_heap);
                    SearchEvent.writeToFile(KEYWORDS, PQ);
                    ReviewEvent.writeToFile(review_heap);
                    System.out.println("written");
                    checkpoint = System.nanoTime();
                }
                
                // Reset heap after 60 seconds
                if (System.nanoTime() - flush_check_point >= 60_000_000_000L) {
                    System.out.println("flush");
                    KEYWORDS = new HashMap<>();
                    PQ = new PriorityQueue<>(Collections.reverseOrder());
                    visit_heap = new Heap(CompareStrategy.TOTAL);
                    rate_heap = new Heap(CompareStrategy.AVERAGE);
                    review_heap = new Heap(CompareStrategy.TOTAL);
                    flush_check_point = System.nanoTime();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createcouconsumer() {
        PROPERTIES.put("bootstrap.servers", BOOTSTRAP_SERVER);
        PROPERTIES.put("group.id", GROUP_ID);
        PROPERTIES.put("key.deserializer", KEY_DESERIALIZER);
        PROPERTIES.put("value.deserializer", VALUE_DESERIALIZER);
        PROPERTIES.put("client.id", "java-consumer");
        PROPERTIES.put("auto.offset.reset", "latest");

        consumer = new KafkaConsumer<>(PROPERTIES);
        consumer.subscribe(Arrays.asList(RATE_TOPIC, VISIT_TOPIC, SEARCH_TOPIC, REVIEW_TOPIC));
    }
}