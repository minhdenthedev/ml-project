package com.speed;

import java.io.BufferedWriter;
import java.io.FileWriter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;

public class ReviewEvent {
    /**
     * Path of output file, don't put it in the directory of intellij or any ide
     * 'cause it won't create any file
     * until you manually stop the program.
     */

    private static final String OUTPUT_DIR = "/home/minh/Work/ml-project/src/main/resources/static/data/speed/review.txt";

    /**
     * TIME_INTERVAL is estimated in nano second, so the below value is equivalent
     * to 10 seconds.
     */
    private static BufferedWriter BW;

    public static void writeToFile(Heap heap) {
        try {
            FileWriter fileWriter1 = new FileWriter(OUTPUT_DIR);
            BW = new BufferedWriter(fileWriter1);

            for (int i = 0; i < 5; i++) {
                if (i == 4) {
                    BW.write(heap.poll() + "");
                    break;
                }
                BW.write(heap.poll() + "\n");
            }
            BW.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void fetchEvents(ConsumerRecord<String, String> record, Heap heap) {
        JSONObject jsonRecord = new JSONObject(record.value());
        heap.update(jsonRecord.getInt("movieId"), 1);
    }
}
