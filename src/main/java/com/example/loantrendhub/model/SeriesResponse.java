package com.example.loantrendhub.model;

import java.util.List;

/** 与前端 index.html 对齐：title/unit/x/series(name+y) */
public record SeriesResponse(
        String title,
        String unit,
        List<String> x,
        List<Series> series
) {
    public record Series(String name, List<Double> y) {}
}
