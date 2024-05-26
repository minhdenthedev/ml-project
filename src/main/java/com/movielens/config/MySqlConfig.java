package com.movielens.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;

@Configuration
public class MySqlConfig {
    @Bean 
    public DataSource source() {
        DataSourceBuilder<?> dSB
            = DataSourceBuilder.create();
        dSB.driverClassName("com.mysql.jdbc.Driver");
 
        // MySQL specific url with database name
        dSB.url("jdbc:mysql://localhost:3306/movielens");
 
        // MySQL username credential
        dSB.username("root");
 
        // MySQL password credential
        dSB.password("1234");
 
        return dSB.build();
    }

    @Bean
    public JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(source());
    }
}
