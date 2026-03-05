package com.example.loantrendhub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;

@SpringBootApplication
public class LoanTrendHubApplication {
    private static final Logger log = LoggerFactory.getLogger(LoanTrendHubApplication.class);

    public static void main(String[] args) throws Exception {
        Files.createDirectories(Path.of("./tmp"));
        SpringApplication.run(LoanTrendHubApplication.class, args);
    }

    @Bean
    CommandLineRunner datasourceBanner(DataSource dataSource) {
        return args -> {
            try (var conn = dataSource.getConnection()) {
                log.info("Datasource url={} driver={}", conn.getMetaData().getURL(), conn.getMetaData().getDriverName());
            }
        };
    }
}