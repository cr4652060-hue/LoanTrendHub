package com.example.loantrendhub.model;

import java.util.List;

/** 与前端 index.html 对齐：title/unit/x/series(name+y) */
public record SeriesResponse(
        String title,
        String unit,
        List<String> x,
        List<Series> series,
        List<String> warnings,
        Meta meta
) {
    public SeriesResponse(String title,
                          String unit,
                          List<String> x,
                          List<Series> series) {
        this(title, unit, x, series, List.of(), Meta.none(series == null ? 0 : series.size()));
    }

    public record Series(String name, List<Double> y) {
    }

    public record Meta(int requestedBranches,
                       int effectiveBranches,
                       boolean truncated,
                       int dropped,
                       String requestedDate,
                       String requestedScope,
                       String requestedMetric,
                       int dataRowCount,
                       boolean allNull) {
        public static Meta none(int branches) {
            return new Meta(branches, branches, false, 0, "", "", "", 0, true);
        }
    }
}
