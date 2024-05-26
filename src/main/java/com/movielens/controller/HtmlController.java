package com.movielens.controller;

import com.movielens.config.KafkaProducerConfig;
import com.movielens.entity.Movie;
import com.movielens.entity.Review;
import com.movielens.entity.events.ActivityEvent;
import com.movielens.entity.events.RateEvent;
import com.movielens.entity.events.ReviewEvent;
import com.movielens.entity.events.SearchEvent;
import com.movielens.entity.events.VisitedEvent;
import com.movielens.entity.form.MovieKeywordSearch;
import com.movielens.entity.form.RatingForm;
import com.movielens.entity.form.ReviewForm;
import com.movielens.repositories.MySqlRepository;
import com.movielens.repositories.ReviewRepository;
import com.movielens.service.JsonKafkaProducer;
import com.movielens.service.Recommender;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.json.JSONObject;;

/**
 *
 * @author hminh
 */
@Controller
public class HtmlController {
    private static Logger logger = LoggerFactory.getLogger(HtmlController.class);
    
    @Autowired
    private ReviewRepository reviewRepository;

    @Autowired
    private MySqlRepository mySqlRepository;

    
    @GetMapping("")
    public String index(Model model) {
        // User-based recommender
        List<RecommendedItem> rec = new ArrayList<>();
        model.addAttribute("form", new MovieKeywordSearch());
        try {
            rec = Recommender.getUserBasedRecommendations(500, 10);
        } catch (Exception e) {
        }
        List<Movie> recMovies = new ArrayList<>();
        for (RecommendedItem item : rec) {
            recMovies.add(mySqlRepository.getMovieById(item.getItemID()));
        }
        model.addAttribute("recMovies", recMovies);
        return "index";
    }
    
    @GetMapping("/movies")
    public String movieHtml(Model model) {
        List<Map<String, Object>> movies = mySqlRepository.getMovieList();
        // Search form
        model.addAttribute("form", new MovieKeywordSearch());

        // Movie list
        model.addAttribute("movies", movies);
        return "movies";
    }
    
    @PostMapping("/movies-search")
    public String movieSearchHtml(@ModelAttribute MovieKeywordSearch form, 
            Model model) {

        // Results
        model.addAttribute("results", 
                mySqlRepository.keywordSearch(form.getKeyword()));
        
        // Form
        model.addAttribute("form", form);

        JsonKafkaProducer.sendMessage("search-events", new SearchEvent(form.getKeyword()));

        return "movie-search";
    }
    
    @GetMapping("/movies/{id}")
    public String getMovie(@PathVariable("id") int id, Model model) {
        // Search form
        model.addAttribute("form", new MovieKeywordSearch());

        // Movie 
        model.addAttribute("movie", mySqlRepository.getMovieById(id));

        // Recommendations
        List<RecommendedItem> similarItems = new ArrayList<>();
        try {
            similarItems = Recommender.getItemBasedRecommendations(id, 10);
        } catch (TasteException e) {
        }
        List<Movie> similarMovies = new ArrayList<>();
        for (RecommendedItem item : similarItems) {
            similarMovies.add(mySqlRepository.getMovieById(item.getItemID()));
        }
        model.addAttribute("similarMovies", similarMovies);

        // Rate form
        model.addAttribute("rateForm", new RatingForm());

        // Review form
        ReviewForm reviewForm = new ReviewForm();
        reviewForm.setMovieId(id);
        reviewForm.setUserId(0);
        model.addAttribute("reviewForm", reviewForm);

        // Reviews
        List<Review> reviews = reviewRepository.findByMovieId(Integer.toString(id));
        model.addAttribute("reviews", reviews);

        // Kafka send message
        JsonKafkaProducer.sendMessage("visited-events", new VisitedEvent(500, id));
        JsonKafkaProducer.sendMessage("activity-events", new ActivityEvent(500, id));

        return "movie-detail";
    }

    // API to handle reviews
    @PostMapping("/movies/{id}/post-review")
    public String reviewMovie(@ModelAttribute ReviewForm form,
            Model model, @PathVariable("id") int id) {

        // String message = Long.toString(form.getUserId()) + "|" + Integer.toString(id) + "|" + form.getBody();
        // producer.send(new ProducerRecord<>("review-events", message));
        JsonKafkaProducer.sendMessage("review-events", new ReviewEvent(500, id, form.getBody()));
        JsonKafkaProducer.sendMessage("activity-events", new ActivityEvent(500, id));
        
        return "redirect:/movies/" + Integer.toString(id);
    }
    
    // API to handle rates
    @PostMapping("/rate-movie/{id}")
    public String rateMovie(@ModelAttribute RatingForm form, 
            Model model, @PathVariable("id") int id) {
        model.addAttribute("form", form);
        Movie movie = mySqlRepository.getMovieById(id);
        model.addAttribute("movie", movie);
        
        // String key = "User 0";
        // String value = Integer.toString(id) + "-" + Integer.toString(form.getRating());
        // producer.send(new ProducerRecord<>("rated-events", null, null, key, value));
        JsonKafkaProducer.sendMessage("rate-events", new RateEvent(500, id, form.getRating()));
        JsonKafkaProducer.sendMessage("activity-events", new ActivityEvent(500, id));

        return "movie-rate";
    }

    @GetMapping("/history-insight")
    public String historyInsight(Model model) {
        model.addAttribute("form", new MovieKeywordSearch());
        return "history";
    }

    @GetMapping("/realtime-insight")
    public String realTimeInsight(Model model) {
        model.addAttribute("form", new MovieKeywordSearch());
        return "realtime";
    }
}
