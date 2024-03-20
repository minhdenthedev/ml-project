/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.config;

/**
 * Giai thich: Class nay dung de cau hinh cho Data Source (HiveRepository) va
 * JdbcTemplate. JdbcTemplate nhan vao mot DataSource object. 
 * @author hminh
 */
import java.io.IOException;

import javax.sql.DataSource;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class HiveConfig {   
    @Bean
    public DataSource getHiveDataSource() throws IOException {
        
//        BasicDataSource dataSource = new BasicDataSource();
//        dataSource.setUrl(this.hiveConnectionURL);
//        dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
//        dataSource.setUsername(this.userName);
//        dataSource.setPassword(this.password);
//
//        return dataSource;
        return DataSourceBuilder.create()
                .driverClassName("org.apache.hive.jdbc.HiveDriver")
                .url("jdbc:hive2://localhost:10000/default")
                .username("")
                .password("")
                .build();
    }

    @Bean(name = "jdbcTemplate")
    public JdbcTemplate getJDBCTemplate() throws IOException {
        return new JdbcTemplate(getHiveDataSource());
    }
}
