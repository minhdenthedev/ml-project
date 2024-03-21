/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.controller;

/**
 * Class nay la RestController, tiep nhan thong tin tu client va xu ly sau do 
 * giao tiep voi data access layer (Service). Sau khi nhan duoc phan hoi tu 
 * Service thi tra lai cho phia nguoi dung.
 * RestAPI, MVC, HTTPs Methods.
 * @author hminh
 */
import com.movielens.service.HiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
@RequestMapping("/hive")
public class HiveController {
    private static Logger logger = LoggerFactory.getLogger(HiveController.class);

    @Autowired
    private HiveService hiveService;
    
    

//    @RequestMapping(value = "/{schema}/tables", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
//    public ResponseEntity<List<Map<String, Object>>> getTablesForSchema(@PathVariable String schema) {
//        List<Map<String, Object>> rows = hiveService.getTables(schema);
//        return new ResponseEntity<>(rows, HttpStatus.OK);
//    }
//
//    @RequestMapping(value = "/schemas", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
//    public ResponseEntity<List<Map<String, Object>>> getSchemas() {
//        List<Map<String, Object>> rows = hiveService.getSchemas();
//        return new ResponseEntity<>(rows, HttpStatus.OK);
//    }
//
//    @RequestMapping(value = "/{schema}/preview/{table}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
//    public ResponseEntity<List<Map<String, Object>>> previewTable(
//            @PathVariable String schema, @PathVariable String table
//    ) {
//        List<Map<String, Object>> rows = hiveService.getTablePreview(schema, table);
//        return new ResponseEntity<>(rows, HttpStatus.OK);
//    }
//    
//    @RequestMapping(value = "{schema}/create/{table}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
//    public ResponseEntity<String> createTable(@PathVariable String schema, @PathVariable String table) {
//        hiveService.createTable(schema, table);
//        return ResponseEntity.ok().body("Create table successfully!");
//    }
//    
//    @RequestMapping(value = "{schema}/select/{row}/from/{table}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE) 
//    public ResponseEntity<List<Map<String, Object>>> selectRowFromTable(
//            @PathVariable String schema,
//            @PathVariable String row,
//            @PathVariable String table
//    ) {
//        List<Map<String, Object>> results = hiveService.selectRowFromTable(
//                schema, row, table
//        );
//        return ResponseEntity.ok().body(results);
//        
//    }
}
