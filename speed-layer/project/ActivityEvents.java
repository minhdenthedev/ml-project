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

public class ActivityEvents {
    /**
     * Output format: id|total|num_occurance|average
     * */
    private static Heap heap_user = new Heap(CompareStrategy.TOTAL);
    private static Heap heap_movie = new Heap(CompareStrategy.TOTAL);

    /**
     * TIME_INTERVAL is estimated in nano second, so the below value is equivalent to 10 seconds.
     * */
    private static final long TIME_INTERVAL = 5_000_000_000L;
    private static BufferedWriter BW_USER;
    private static BufferedWriter BW_MOVIE;
    private static int PROCESS_ID = 0;
    private static DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    /**
     * Path of output file, don't put it in the directory of intellij or any ide 'cause it won't create any file
     * until you manually stop the program.
     * */


    private static final String OUTPUT_DIR_USER = "/home/giap/test/activity-events/user";
    private static final String OUTPUT_DIR_MOVIE = "/home/giap/test/activity-events/movie";

    private static final Properties PROPERTIES = new Properties();
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "activity-events";
    private static final String GROUP_ID = "activity-events-group";
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
                    heap_movie.update(jsonRecord.getInt("movieId"), 1);
                    heap_user.update(jsonRecord.getInt("userId"), 1);

                    // Trigger after 10 seconds
                    if (System.nanoTime() - checkpoint >= TIME_INTERVAL) {
                        FileWriter fileWriter1 = new FileWriter(OUTPUT_DIR_USER + PROCESS_ID + ".txt");
                        BW_USER = new BufferedWriter(fileWriter1);
                        FileWriter fileWriter2 = new FileWriter(OUTPUT_DIR_MOVIE + PROCESS_ID + ".txt");
                        BW_MOVIE = new BufferedWriter(fileWriter2);

                        for (int i = 0; i < 10; i++) {
                            BW_USER.write(heap_user.poll() + "\n");
                            BW_MOVIE.write(heap_movie.poll() + "\n");
                        }
                        BW_USER.flush();
                        BW_MOVIE.flush();
                        System.out.println("Done: Id = " + PROCESS_ID + " - " +
                                DTF.format(LocalDateTime.now()));
                        heap_user = new Heap(CompareStrategy.TOTAL);
                        heap_movie = new Heap(CompareStrategy.TOTAL);
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
