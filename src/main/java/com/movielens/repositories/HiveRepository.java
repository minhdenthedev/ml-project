/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.repositories;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class nay dung de giao tiep va thao tac truc tiep len HiveServer2.
 * Trong class nay co:
 * - Logger
 * - JdbcTemplate: dung de thuc thi truy van. Cau hinh template nay o trong com.movielens.service.HiveService.
 * - Mot so query mau de test.
 * @author hminh
 */
@Service
public class HiveRepository {
    private static final Logger logger = LoggerFactory.getLogger(HiveRepository.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String SHOW_TABLES_QUERY = "show tables";
    private static final String SHOW_DATABASES_QUERY = "show databases";
    private static final String TABLE_PLACEHOLDER = "{table}";
    private static final String ROW_PLACEHOLDER = "{row}";
    private static final String PREVIEW_TABLE_QUERY = "select * from " + TABLE_PLACEHOLDER + " limit 100";
    private static final String CREATE_TABLE_QUERY = "create table if not exists " + TABLE_PLACEHOLDER
            + " (KEY INT, VALUE STRING) PARTITIONED BY (CTIME DATE) ROW "
            + "FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' "
            + "STORED AS TEXTFILE";

    public List<Map<String, Object>> getTables(String schema) {
        jdbcTemplate.execute("use " + schema);
        logger.info(SHOW_TABLES_QUERY);
        return jdbcTemplate.queryForList(SHOW_TABLES_QUERY);
    }

    public List<Map<String, Object>> getSchemas() {
        logger.info(SHOW_DATABASES_QUERY);
        return jdbcTemplate.queryForList(SHOW_DATABASES_QUERY);
    }

    public List<Map<String, Object>> getTablePreview(String schema, String table) {
        jdbcTemplate.execute("use " + schema);
        String query = PREVIEW_TABLE_QUERY.replace(TABLE_PLACEHOLDER, table);
        logger.info(query);
        return jdbcTemplate.queryForList(query);
    }
    
    public void createTable(String schema, String table) {
        jdbcTemplate.execute("use " + schema);
        String query = CREATE_TABLE_QUERY.replace(TABLE_PLACEHOLDER, table);
        logger.info(query);
        jdbcTemplate.execute(query);
    }
    
    public List<Map<String, Object>> selectRowFromTable(String schema, String row, String table) {
        jdbcTemplate.execute("use " + schema);
        String query = "select " + 
            row + " from " + table + " limit 100";
        return jdbcTemplate.queryForList(query);
    }
}
