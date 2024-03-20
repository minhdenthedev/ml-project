/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.service;

/**
 * Class trung gian giua Controller va Service.
 * @author hminh
 */
import com.movielens.repositories.HiveRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class HiveService {

    @Autowired
    private HiveRepository hiveRepository;

    public List<Map<String, Object>> getTables(String schema) {
        return hiveRepository.getTables(schema);
    }

    public List<Map<String, Object>> getSchemas() {
        return hiveRepository.getSchemas();
    }

    public List<Map<String, Object>> getTablePreview(String schema, String table) {
        return hiveRepository.getTablePreview(schema, table);
    }
    
    public void createTable(String schema, String table) {
        hiveRepository.createTable(schema, table);
    }
    
    public List<Map<String, Object>> selectRowFromTable(String schema, String row, String table) {
        return hiveRepository.selectRowFromTable(schema, row, table);
    }
}
