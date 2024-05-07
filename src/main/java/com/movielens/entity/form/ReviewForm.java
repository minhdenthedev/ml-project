package com.movielens.entity.form;

public class ReviewForm {
    private long movieId;
    private String title;
    private String body;

    public String getTitle() {
        return title;
    }

    public String getBody() {
        return body;
    }

    public long getMovieId() {
        return movieId;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public void setMovieId(long movieId) {
        this.movieId = movieId;
    }

}
