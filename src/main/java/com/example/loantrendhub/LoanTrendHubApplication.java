package com.example.loantrendhub;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.file.Files;
import java.nio.file.Path;

@SpringBootApplication
public class LoanTrendHubApplication {

    public static void main(String[] args) throws Exception {
        // 确保 ./data 和 ./tmp 存在，避免 SQLite 报 “path does not exist”
        Files.createDirectories(Path.of("./data"));
        Files.createDirectories(Path.of("./tmp"));

        SpringApplication.run(LoanTrendHubApplication.class, args);
    }
}