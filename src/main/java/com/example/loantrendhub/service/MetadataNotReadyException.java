package com.example.loantrendhub.service;

public class MetadataNotReadyException extends RuntimeException {
    public MetadataNotReadyException(String message) {
        super(message);
    }
}