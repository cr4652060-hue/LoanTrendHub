package com.example.loantrendhub.controller;

import com.example.loantrendhub.service.MetadataNotReadyException;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(MetadataNotReadyException.class)
    public ResponseEntity<Map<String, Object>> handleMetadataNotReady(MetadataNotReadyException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                "code", "META_NOT_READY",
                "message", ex.getMessage()
        ));
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<Map<String, Object>> handleDataAccess(DataAccessException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                "code", "DB_NOT_READY",
                "message", "数据库初始化未完成，请检查 schema-mysql.sql 是否执行"
        ));
    }
}