package com.movielens.entity.form;

public class ReviewForm {
    private long movieId;
    private long userId;
    private String body;


    public String getBody() {
        return body;
    }

    public long getMovieId() {
        return movieId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long id) {
        this.userId = id;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public void setMovieId(long movieId) {
        this.movieId = movieId;
    }

}
