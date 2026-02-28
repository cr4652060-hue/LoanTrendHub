package com.example.loantrendhub.model;

import java.util.List;
import java.util.Map;

/** 与前端 index.html 对齐：metrics/branches/data/min/max */
public record HeatmapResponse(
        String date,
        String scope,
        List<String> metrics,
        List<String> branches,
        Double min,
        Double max,
        List<List<Object>> data,
        Map<String, MetricMeta> metricDefs
) {
    public record MetricMeta(String name, String unit) {}
}
