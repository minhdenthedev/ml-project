package org.example;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.ds.CompareStrategy;
import org.example.ds.Heap;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;

public class RateEvents {
    /**
     * Output format: id|total|num_occurance|average
     * */
    private static Heap heap = new Heap(CompareStrategy.AVERAGE);

    /**
     * TIME_INTERVAL is estimated in nano second, so the below value is equivalent to 10 seconds.
     * */
    private static final long TIME_INTERVAL = 10_000_000_000L;
    private static BufferedWriter BW;
    private static int PROCESS_ID = 0;
    private static DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    /**
     * Path of output file, don't put it in the directory of intellij or any ide 'cause it won't create any file
     * until you manually stop the program.
     * */
    private static final String OUTPUT_DIR = "/home/giap/test/rate-events/";

    private static final Properties PROPERTIES = new Properties();
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "rate-events";
    private static final String GROUP_ID = "rate-events-group";
    private static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    private static KafkaConsumer<String, String> CONSUMER;

    public static void main(String[] args) {
        createConsumer();
        System.out.println("Ready");

        try {
            long checkpoint = System.nanoTime();
            while (true) {
                ConsumerRecords<String, String> records = CONSUMER.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    JSONObject jsonRecord = new JSONObject(record.value());
                    heap.update(jsonRecord.getInt("movieId"), jsonRecord.getInt("rating"));

                    // Trigger after 10 seconds
                    if (System.nanoTime() - checkpoint >= TIME_INTERVAL) {
                        FileWriter fileWriter = new FileWriter(OUTPUT_DIR + PROCESS_ID + ".txt");
                        BW = new BufferedWriter(fileWriter);

                        for (int i = 0; i < 10; i++) {
                            BW.write(heap.poll() + "\n");
                        }
                        BW.flush();
                        System.out.println("Done: Id = " + PROCESS_ID + " - " +
                                DTF.format(LocalDateTime.now()));
                        heap = new Heap(CompareStrategy.AVERAGE);
                        PROCESS_ID++;
                        checkpoint = System.nanoTime();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createConsumer() {
        PROPERTIES.put("bootstrap.servers", BOOTSTRAP_SERVER);
        PROPERTIES.put("group.id", GROUP_ID);
        PROPERTIES.put("key.deserializer", KEY_DESERIALIZER);
        PROPERTIES.put("value.deserializer", VALUE_DESERIALIZER);
        PROPERTIES.put("auto.offset.reset", "latest");

        CONSUMER = new KafkaConsumer<>(PROPERTIES);
        CONSUMER.subscribe(Collections.singletonList(TOPIC));
    }
}
