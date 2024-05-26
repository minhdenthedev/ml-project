package com.speed;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;

public class SearchEvent {
    /**
     * Path of output file, don't put it in the directory of intellij or any ide
     * 'cause it won't create any file
     * until you manually stop the program.
     */

    private static final String OUTPUT_DIR = "/home/minh/Work/ml-project/src/main/resources/static/data/speed/search.txt";

    /**
     * TIME_INTERVAL is estimated in nano second, so the below value is equivalent
     * to 10 seconds.
     */
    private static BufferedWriter BW;

    public static void writeToFile(Map<String, Integer> KEYWORDS, PriorityQueue<Keyword> PQ) {
        try {
            FileWriter fileWriter1 = new FileWriter(OUTPUT_DIR);
            BW = new BufferedWriter(fileWriter1);

            for (String key : KEYWORDS.keySet()) {
                System.out.println(key);
                PQ.add(new Keyword(key, KEYWORDS.get(key)));
                System.out.println(PQ.size());
            }
            int size = PQ.size();
            for (int i = 0; i < size; i++) {
                Keyword keyword1 = PQ.poll();
                if (i == size - 1) {
                    BW.write(keyword1.getValue() + "|" + keyword1.getFreq() + "");
                    break;
                }
                BW.write(keyword1.getValue() + "|" + keyword1.getFreq() + "\n");
            }
            BW.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void fetchEvents(ConsumerRecord<String, String> record, Map<String, Integer> KEYWORDS) {
        JSONObject jsonObject = new JSONObject(record.value());
        String[] keywords = parse(jsonObject.getString("keyword"));
        for (String keyword : keywords) {
            KEYWORDS.putIfAbsent(keyword, 0);
            KEYWORDS.put(keyword, 1 + KEYWORDS.get(keyword));
        }
    }

    private static String[] parse(String line) {
        return line.split("[\\p{Punct}\\s]+");
    }
}
