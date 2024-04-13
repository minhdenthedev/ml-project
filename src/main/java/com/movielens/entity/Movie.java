/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.entity;

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

    public Movie(Map<String, Object> map) {
        this.id = (Integer) map.get("u_item.movie_id");
        map.remove("u_item.movie_id");
        this.title = (String) map.get("u_item.movie_title");
        map.remove("u_item.movie_title");
        this.releaseDate = (String) map.get("u_item.release_date");
        map.remove("u_item.release_date");
        this.imdbLink = (String) map.get("u_item.imdb_link");
        map.remove("u_item.imdb_link");
        this.videoReleaseDate = (String) map.get("u_item.video_release_date");
        if (this.videoReleaseDate == null) {
            this.videoReleaseDate = "unknown";
        }
        map.remove("u_item.video_release_date");
        // Append the genres
        int counter = 0;
        StringBuilder sb = new StringBuilder();
        for (String key : map.keySet()) {
            if (map.get(key) == Byte.valueOf("1")) {
                String genre = key.replace("u_item.", "");
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

}
