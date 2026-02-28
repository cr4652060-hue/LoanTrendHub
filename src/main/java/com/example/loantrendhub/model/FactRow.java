package com.example.loantrendhub.model;

public record FactRow(
        String bizDate,
        String scope,
        String branch,
        String metric,
        Double value,
        String sourceFile
) {}