package com.example.loantrendhub.model;

import java.util.List;
import java.util.Map;

public record HeatmapResponse(
        String date,
        String scope,
        List<String> x,
        List<String> y,
        List<List<Object>> data,
        Map<String, MetricMeta> metricDefs
) {
    public record MetricMeta(String name, String unit) {}
}