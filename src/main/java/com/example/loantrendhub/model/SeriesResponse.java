package com.example.loantrendhub.model;

import java.util.List;
import java.util.Map;

public record SeriesResponse(
        List<String> x,
        List<SeriesItem> series,
        Map<String, Object> metric
) {
    public record SeriesItem(String name, List<Double> data) {}
}