package com.example.loantrendhub.model;

public record MetricDef(
        String metric,
        String name,
        String unit,
        String kind,
        String baseMetric
) {}