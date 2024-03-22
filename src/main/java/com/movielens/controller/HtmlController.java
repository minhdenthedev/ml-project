/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.controller;

import com.movielens.entity.form.MovieKeywordSearch;
import com.movielens.service.HiveService;
import java.util.List;
import java.util.Map;
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
    private static Logger logger = LoggerFactory.getLogger(HiveController.class);

    
    @Autowired
    private HiveService hiveService;
    
    @GetMapping("")
    public String index(Model model) {
        List<Object> info = hiveService.getGenralInfo();
        model.addAttribute("info", info.get(0));
        model.addAttribute("ratingPercentages", info.get(1));
        model.addAttribute("topFive", info.get(2));
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
        return "movie-search";
    }
    
    @GetMapping("/movies/{id}")
    public String getMovie(@PathVariable("id") int id, Model model) {
        model.addAttribute("movie", hiveService.getMovieById(id));
    }
}
