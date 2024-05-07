/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.controller;

import com.movielens.config.KafkaProducerConfig;
import com.movielens.entity.Movie;
import com.movielens.entity.form.MovieKeywordSearch;
import com.movielens.entity.form.RatingForm;
import com.movielens.entity.form.ReviewForm;
import com.movielens.service.HiveService;
import com.movielens.service.Recommender;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

/**
 *
 * @author hminh
 */
@Controller
public class HtmlController {
    private static Logger logger = LoggerFactory.getLogger(HtmlController.class);
    
    final private static Producer<String, String> producer 
            = new KafkaProducer<>(new KafkaProducerConfig());
    
    
    @Autowired
    private HiveService hiveService;
    
    // private MongoService mongoService = new MongoService();
    
    @GetMapping("")
    public String index(Model model) {
        List<RecommendedItem> rec = new ArrayList<>();
        model.addAttribute("form", new MovieKeywordSearch());
        try {
            rec = Recommender.getItemBasedRecommendations(300, 10);
        } catch (TasteException e) {
        }
        List<Movie> recMovies = new ArrayList<>();
        for (RecommendedItem item : rec) {
            recMovies.add(hiveService.getMovieById(item.getItemID()));
        }
        model.addAttribute("recMovies", recMovies);
        return "index";
    }
    
    @GetMapping("/movies")
    public String movieHtml(Model model) {
        List<Map<String, Object>> movies = hiveService.getAllMovies();
        model.addAttribute("form", new MovieKeywordSearch());
        model.addAttribute("movies", movies);
        return "movies";
    }
    
    @PostMapping("/movies-search")
    public String movieSearchHtml(@ModelAttribute MovieKeywordSearch form, 
            Model model) {
        model.addAttribute("results", 
                hiveService.keywordSearch(form.getKeyword()));
        model.addAttribute("form", form);

        String key = "User 0";
        String value = form.getKeyword();
        producer.send(new ProducerRecord<>("searched-events", null, null, key, value));

        return "movie-search";
    }
    
    @GetMapping("/movies/{id}")
    public String getMovie(@PathVariable("id") int id, Model model) {
        model.addAttribute("form", new MovieKeywordSearch());
        model.addAttribute("movie", hiveService.getMovieById(id));
        List<RecommendedItem> similarItems = new ArrayList<>();
        try {
            similarItems = Recommender.getItemBasedRecommendations(id, 10);
        } catch (TasteException e) {
        }
        List<Movie> similarMovies = new ArrayList<>();
        for (RecommendedItem item : similarItems) {
            similarMovies.add(hiveService.getMovieById(item.getItemID()));
        }
        model.addAttribute("similarMovies", similarMovies);
        model.addAttribute("rateForm", new RatingForm());

        ReviewForm reviewForm = new ReviewForm();
        reviewForm.setMovieId(id);
        model.addAttribute("reviewForm", reviewForm);

        // ArrayList<Review> reviewList = mongoService.getReviews(id);
        // model.addAttribute("reviewList", reviewList);

        String key = "User 0";
        String value = Integer.toString(id); 
        producer.send(new ProducerRecord<>("visited-events", null, null, key, value));

        return "movie-detail";
    }

    @PostMapping("/movies/{id}/post-review")
    public String reviewMovie(@ModelAttribute ReviewForm form,
            Model model, @PathVariable("id") int id) {

        String key = "User 0";
        String value = form.getTitle() + "|||" + form.getBody();
        producer.send(new ProducerRecord<>("reviews-" + Integer.toString(id), null, null, key, value));
        
        return "redirect:/movies/" + Integer.toString(id);
    }
    
    @PostMapping("/rate-movie/{id}")
    public String rateMovie(@ModelAttribute RatingForm form, 
            Model model, @PathVariable("id") int id) {
        model.addAttribute("form", form);
        Movie movie = hiveService.getMovieById(id);
        model.addAttribute("movie", movie);
        
        String key = "User 0";
        String value = Integer.toString(id) + "-" + Integer.toString(form.getRating());
        producer.send(new ProducerRecord<>("rated-events", null, null, key, value));

        return "movie-rate";
    }
}
