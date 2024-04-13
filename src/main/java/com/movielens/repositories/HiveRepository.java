/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.repositories;

import java.util.HashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.lang.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class nay dung de giao tiep va thao tac truc tiep len HiveServer2. Trong
 * class nay co: - Logger - JdbcTemplate: dung de thuc thi truy van. Cau hinh
 * template nay o trong com.movielens.service.HiveService. - Mot so query mau de
 * test.
 *
 * @author hminh
 */
@Service
public class HiveRepository {

    private static final Logger logger = LoggerFactory.getLogger(HiveRepository.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public Map<String, Object> getUInfo() {
        Map<String, Object> map = new HashMap<>();

        jdbcTemplate.execute("use default");
        String query = "select * from u_info";
        logger.info(query);
        List<Map<String, Object>> result = jdbcTemplate.queryForList(query);

        map.put("users", result.get(0).get("u_info.users"));
        map.put("movies", result.get(0).get("u_info.items"));
        map.put("ratings", result.get(0).get("u_info.ratings"));

        return map;
    }

    public List<Map<String, Object>> getMovieRatingPercentage() {
        jdbcTemplate.execute("use default");
        String query = "select rating, count(*) as number from u_data group by rating";
        List<Map<String, Object>> result = jdbcTemplate.queryForList(query);
        return result;
    }

    public List<Map<String, Object>> getTopFivePopularMovie() {
        jdbcTemplate.execute("use default");
        String query = "select movie_title, count(*) as user_rated \n"
                + "from u_data \n"
                + "join u_item\n"
                + "on u_data.movie_id = u_item.movie_id \n"
                + "group by movie_title\n"
                + "order by user_rated desc\n"
                + "limit 5";
        return jdbcTemplate.queryForList(query);
    }
    
    public List<Map<String, Object>> keywordSearch(String keyword) {
        keyword = WordUtils.capitalizeFully(keyword);
        keyword.replace(' ', '%');
        jdbcTemplate.execute("use default");
        String query = "select movie_id, release_date, movie_title "
                + "from u_item where "
                + "movie_title like \'%" + keyword + "%\'";
        
        return jdbcTemplate.queryForList(query);
    }
    
    public List<Map<String, Object>> getAllMovies() {
        jdbcTemplate.execute("use default");
        String query = "select movie_title, release_date,"
                + " round(avg(rating), 2) as avg_rate "
                + "from u_data join u_item on u_data.movie_id = u_item.movie_id "
                + "group by movie_title, release_date limit 25";
        return jdbcTemplate.queryForList(query);
    }
    
    public List<Map<String, Object>> getMovieById(long id) {
        jdbcTemplate.execute("use default");
        String query = "select * from u_item "
                + "where movie_id = " + id;
        return jdbcTemplate.queryForList(query);
    }
    

//    private static final String SHOW_TABLES_QUERY = "show tables";
//    private static final String SHOW_DATABASES_QUERY = "show databases";
//    private static final String TABLE_PLACEHOLDER = "{table}";
//    private static final String ROW_PLACEHOLDER = "{row}";
//    private static final String PREVIEW_TABLE_QUERY = "select * from " + TABLE_PLACEHOLDER + " limit 100";
//    private static final String CREATE_TABLE_QUERY = "create table if not exists " + TABLE_PLACEHOLDER
//            + " (KEY INT, VALUE STRING)"
//            + "FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' "
//            + "STORED AS TEXTFILE";
//
//    public List<Map<String, Object>> getTables(String schema) {
//        jdbcTemplate.execute("use " + schema);
//        logger.info(SHOW_TABLES_QUERY);
//        return jdbcTemplate.queryForList(SHOW_TABLES_QUERY);
//    }
//
//    public List<Map<String, Object>> getSchemas() {
//        logger.info(SHOW_DATABASES_QUERY);
//        return jdbcTemplate.queryForList(SHOW_DATABASES_QUERY);
//    }
//
//    public List<Map<String, Object>> getTablePreview(String schema, String table) {
//        jdbcTemplate.execute("use " + schema);
//        String query = PREVIEW_TABLE_QUERY.replace(TABLE_PLACEHOLDER, table);
//        logger.info(query);
//        return jdbcTemplate.queryForList(query);
//    }
//    
//    public void createTable(String schema, String table) {
//        jdbcTemplate.execute("use " + schema);
//        String query = CREATE_TABLE_QUERY.replace(TABLE_PLACEHOLDER, table);
//        logger.info(query);
////        jdbcTemplate.execute(query);
////    }
//    
//    public List<Map<String, Object>> selectRowFromTable(String schema, String row, String table) {
//        jdbcTemplate.execute("use " + schema);
//        String query = "select " + 
//            row + " from " + table + " limit 100";
//        return jdbcTemplate.queryForList(query);
//    }
//    
}
