package com.movielens.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "reviews")
public class Review {
    
    @Id
    public String id;

    String body;
    String userId;
    String movieId;
    String attitude;
    
    public String getUserId() {
        return userId;
    }
    public void setUserId(String userId) {
        this.userId = userId;
    }
    public String getMovieId() {
        return movieId;
    }
    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public String getBody() {
        return body;
    }
    public void setBody(String body) {
        this.body = body;
    }
    
    public String getAttitude() {
        return attitude;
    }
    public void setAttitude(String attitude) {
        this.attitude = attitude;
    }
}
