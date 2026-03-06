package com.example.loantrendhub.controller;

import com.example.loantrendhub.service.MetadataNotReadyException;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartException;

import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(MetadataNotReadyException.class)
    public ResponseEntity<Map<String, Object>> handleMetadataNotReady(MetadataNotReadyException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                "code", "DB_NOT_READY",
                "message", ex.getMessage()
        ));
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<Map<String, Object>> handleDataAccess(DataAccessException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                "code", "DB_NOT_READY",
                "message", "\u6570\u636e\u5e93\u521d\u59cb\u5316\u672a\u5b8c\u6210\uff0c\u8bf7\u68c0\u67e5 schema-mysql.sql \u662f\u5426\u6267\u884c"
        ));
    }

    @ExceptionHandler({MaxUploadSizeExceededException.class, MultipartException.class})
    public ResponseEntity<Map<String, Object>> handleUploadLimit(Exception ex) {
        return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(Map.of(
                "code", "UPLOAD_TOO_LARGE",
                "message", "\u4e0a\u4f20\u8d85\u8fc7\u9650\u5236\uff1a\u5355\u6587\u4ef6 20MB\uff0c\u603b\u8bf7\u6c42 1GB\uff0c\u5355\u6b21\u6700\u591a 200 \u4e2a\u6587\u4ef6"
        ));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                "code", "BAD_REQUEST",
                "message", ex.getMessage() == null ? "\u8bf7\u6c42\u53c2\u6570\u4e0d\u5408\u6cd5" : ex.getMessage()
        ));
    }
}