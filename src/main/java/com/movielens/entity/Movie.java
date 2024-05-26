/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.entity;

import java.math.BigDecimal;
import java.util.Map;

/**
 *
 * @author minh
 */
public class Movie {

    private int id;
    private String title;
    private String releaseDate;
    private String imdbLink;
    private String genres;
    private String videoReleaseDate;
    private BigDecimal avgRate;
    private Long views;

    public Movie(Map<String, Object> map) {
        this.id = (Integer) map.get("movie_id");
        map.remove("movie_id");
        this.title = (String) map.get("movie_title");
        map.remove("movie_title");
        this.releaseDate = (String) map.get("release_date_temp");
        map.remove("release_date_temp");
        this.imdbLink = (String) map.get("imdb_link");
        map.remove("imdb_link");
        this.videoReleaseDate = (String) map.get("video_release_date");
        if (this.videoReleaseDate == null) {
            this.videoReleaseDate = "unknown";
        }
        // Append the genres
        int counter = 0;
        StringBuilder sb = new StringBuilder();
        for (String key : map.keySet()) {
            if (map.get(key) == (Integer) 1) {
                String genre = key.replace("_", "-");
                if (counter == 0) {
                    sb.append(genre);
                } else {
                    sb.append(", ");
                    sb.append(genre);
                }
                counter++;
            }
        }
        
        this.genres = sb.toString();
        this.avgRate = (BigDecimal) map.get("avg_rate");
        this.views = (Long) map.get("views");
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(String releaseDate) {
        this.releaseDate = releaseDate;
    }

    public String getImdbLink() {
        return imdbLink;
    }

    public void setImdbLink(String imdbLink) {
        this.imdbLink = imdbLink;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }

    public BigDecimal getAvgRate() {
        return avgRate;
    }

    public Long getViews() {
        return views;
    }
}
