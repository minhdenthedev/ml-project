package com.movielens.entity.events;

public record VisitedEvent(int userId, int movieId) {
}