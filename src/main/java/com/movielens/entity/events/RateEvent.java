package com.movielens.entity.events;

public record RateEvent(int userId, int movieId, int rate) {
    
}
