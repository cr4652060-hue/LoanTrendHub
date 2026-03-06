package com.example.loantrendhub.model;

import java.util.List;
import java.util.Map;

/** 与前端 index.html 对齐：metrics/branches/data/min/max + meta */
public record HeatmapResponse(
        String date,
        String scope,
        List<String> metrics,
        List<String> branches,
        Double min,
        Double max,
        List<List<Object>> data,
        List<Cell> cells,
        Map<String, MetricMeta> metricDefs,
        Meta meta
) {
    public record MetricMeta(String name, String unit) {
    }

    public record Cell(String branch, String metric, Double val, boolean hasData) {
    }

    public record Meta(String requestedDate,
                       String requestedScope,
                       String requestedMetric,
                       int dataRowCount,
                       boolean allNull) {
    }
}
