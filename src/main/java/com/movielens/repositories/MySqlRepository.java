package com.movielens.repositories;

import org.apache.commons.lang.WordUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.movielens.entity.Movie;

import java.util.*;

@Repository
public class MySqlRepository {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> getMovieList() {
        return jdbcTemplate.queryForList("select u_data.movie_id, movie_title, release_date_temp, avg(rating) as avg_rate " + 
                                "from u_item join u_data on u_item.movie_id = u_data.movie_id " + 
                                "group by u_item.movie_id order by avg_rate desc limit 25");
    }

    public List<Map<String, Object>> keywordSearch(String keyword) {
        keyword = WordUtils.capitalizeFully(keyword);
        keyword.replace(' ', '%');
        String query = "select movie_id, release_date_temp, movie_title "
                + "from u_item where "
                + "movie_title like \'%" + keyword + "%\'";
        
        return jdbcTemplate.queryForList(query);
    }

    public String getMovieTitleById(long id) {
        jdbcTemplate.execute("use default");
        String query = "select movie_title from u_item "
                + "where movie_id = " + id;
        List<Map<String, Object>> result = jdbcTemplate.queryForList(query);
        return (String) result.get(0).get("movie_title");
    }

    public Movie getMovieById(long id) {
        String query = "select u_item.*, round(avg(rating), 2) as avg_rate, count(*) as views from u_item join u_data on u_item.movie_id = u_data.movie_id where u_data.movie_id = " + id + " group by movie_id";
        List<Map<String, Object>> result = jdbcTemplate.queryForList(query);
        Map<String, Object> map = result.get(0);
        Movie movie = new Movie(map);
        return movie;
    }
}
