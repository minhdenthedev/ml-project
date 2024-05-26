package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.ds.Keyword;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class SearchedEvents {
    private static Map<String, Integer> KEYWORDS = new HashMap<>();
    private static PriorityQueue<Keyword> PQ = new PriorityQueue<>(Collections.reverseOrder());

    /**
     * TIME_INTERVAL is estimated in nano second, so the below value is equivalent to 10 seconds.
     * */
    private static final long TIME_INTERVAL = 20_000_000_000L;
    private static BufferedWriter BW;
    private static int PROCESS_ID = 0;
    private static DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    /**
     * Path of output file, don't put it in the directory of intellij or any ide 'cause it won't create any file
     * until you manually stop the program.
     * */
    private static final String OUTPUT_DIR = "/home/giap/test/searched-events/";

    private static final Properties PROPERTIES = new Properties();
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "searched-events";
    private static final String GROUP_ID = "searched-events-group";
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
                    String[] keywords = parse(record.value());
                    for (String keyword : keywords) {
                        KEYWORDS.putIfAbsent(keyword, 0);
                        KEYWORDS.put(keyword, 1 + KEYWORDS.get(keyword));
                    }

                    // Trigger after 10 seconds
                    if (System.nanoTime() - checkpoint >= TIME_INTERVAL) {
                        FileWriter fileWriter = new FileWriter(OUTPUT_DIR + PROCESS_ID + ".txt");
                        BW = new BufferedWriter(fileWriter);

                        for (String key : KEYWORDS.keySet()) {
                            PQ.add(new Keyword(key, KEYWORDS.get(key)));
                        }
                        for (int i = 0; i < 10; i++) {
                            Keyword keyword1 = PQ.poll();
                            BW.write(keyword1.getValue() + "|" + keyword1.getFreq() + "\n");
                        }

                        BW.flush();
                        System.out.println("Done: Id = " + PROCESS_ID + " - " +
                                DTF.format(LocalDateTime.now()));
                        // Reset the FREQUENCY_MAP for the new time interval
                        KEYWORDS = new HashMap<>();
                        PQ = new PriorityQueue<>(Collections.reverseOrder());
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

    private static String[] parse(String line) {
        return line.split("[\\p{Punct}\\s]+");
    }
}
