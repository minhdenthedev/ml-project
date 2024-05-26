package com.movielens.entity.events;

public record ReviewEvent(int userId, int movieId, String body) {
    
}
