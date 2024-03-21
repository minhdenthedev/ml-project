/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.controller;

import com.movielens.service.HiveService;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

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
        
        return "index";
    }
}
