package com.speed;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class ActivityEvent {
    /**
     * Path of output file, don't put it in the directory of intellij or any ide
     * 'cause it won't create any file
     * until you manually stop the program.
     */

    private static final String OUTPUT_DIR_USER = "/home/minh/Work/ml-project/speed-results/activity/user.txt";
    private static final String OUTPUT_DIR_MOVIE = "/home/minh/Work/ml-project/speed-results/activity/movie.txt";

    /**
     * TIME_INTERVAL is estimated in nano second, so the below value is equivalent
     * to 10 seconds.
     */
    private static BufferedWriter BW_USER;
    private static BufferedWriter BW_MOVIE;

    public static void writeToFile(Heap heap_movie, Heap heap_user) {
        try {
            FileWriter fileWriter1 = new FileWriter(OUTPUT_DIR_USER);
            BW_USER = new BufferedWriter(fileWriter1);
            FileWriter fileWriter2 = new FileWriter(OUTPUT_DIR_MOVIE);
            BW_MOVIE = new BufferedWriter(fileWriter2);
            
            for (int i = 0; i < 10; i++) {
                BW_USER.write(heap_user.poll() + "\n");
                BW_MOVIE.write(heap_movie.poll() + "\n");
            }
            BW_USER.flush();
            BW_MOVIE.flush();
            heap_movie = new Heap(CompareStrategy.TOTAL);
            heap_user = new Heap(CompareStrategy.TOTAL);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void fetchEvents(ConsumerRecord<String, String> record, Heap heap_movie, Heap heap_user) {
        JSONObject jsonRecord = new JSONObject(record.value());
        heap_movie.update(jsonRecord.getInt("movieId"), 1);
        heap_user.update(jsonRecord.getInt("userId"), 1);
    }
}