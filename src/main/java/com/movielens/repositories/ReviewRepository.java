package com.movielens.repositories;

import java.util.List;
import org.springframework.data.mongodb.repository.MongoRepository;
import com.movielens.entity.Review;

public interface ReviewRepository extends MongoRepository<Review, String>{
    
    public List<Review> findByMovieId(String movieId);
}
