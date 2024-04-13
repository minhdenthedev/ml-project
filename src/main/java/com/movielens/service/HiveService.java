/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.service;

/**
 * Class trung gian giua Controller va Service.
 *
 * @author hminh
 */
import com.movielens.entity.Movie;
import com.movielens.entity.RatingPercentage;
import com.movielens.repositories.HiveRepository;
import java.util.ArrayList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class HiveService {
    private static Logger logger = LoggerFactory.getLogger(HiveService.class);

    @Autowired
    private HiveRepository hiveRepository;

    public List<Object> getGenralInfo() {
        List<Object> list = new ArrayList<>();

        // Add general information
        list.add(hiveRepository.getUInfo());

        // Add rating percentages
        List<Record> ratingPercentages = new ArrayList<>();
        List<Map<String, Object>> listMap = hiveRepository.getMovieRatingPercentage();
        for (Map map : listMap) {

            Integer rating = (int) map.get("rating");
            Long number = (long) map.get("number");
            String percentage = Long.toString(number / 1000) + "%";
            ratingPercentages.add(new RatingPercentage(rating, number, percentage));
        }

        list.add(ratingPercentages);
        
        // Add top five popular movies
        list.add(hiveRepository.getTopFivePopularMovie());
        
        return list;
    }
    
    public List<Map<String, Object>> keywordSearch(String keyword) {
        return hiveRepository.keywordSearch(keyword);
    }
    
    public List<Map<String, Object>> getAllMovies() {
        return hiveRepository.getAllMovies();
    }
    
    public Movie getMovieById(long id) {
        Map<String, Object> map = hiveRepository.getMovieById(id).get(0);
        Movie movie = new Movie(map);
        return movie;
    }

//    public List<Map<String, Object>> getTables(String schema) {
//        return hiveRepository.getTables(schema);
//    }
//
//    public List<Map<String, Object>> getSchemas() {
//        return hiveRepository.getSchemas();
//    }
//
//    public List<Map<String, Object>> getTablePreview(String schema, String table) {
//        return hiveRepository.getTablePreview(schema, table);
//    }
//    
//    public void createTable(String schema, String table) {
//        hiveRepository.createTable(schema, table);
//    }
//    
//    public List<Map<String, Object>> selectRowFromTable(String schema, String row, String table) {
//        return hiveRepository.selectRowFromTable(schema, row, table);
//    }
}
