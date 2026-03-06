package com.example.loantrendhub.controller;

import com.example.loantrendhub.service.QueryService;
import com.example.loantrendhub.util.TextCleanUtil;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class MetaController {
    private final QueryService queryService;

    public MetaController(QueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/scopes")
    public List<Map<String, Object>> scopes() {
        Map<String, Object> dateRange = queryService.dateRange();
        String targetDate = String.valueOf(dateRange.getOrDefault("max", ""));
        List<Map<String, Object>> stats = queryService.scopeStats(targetDate);
        if (!stats.isEmpty()) {
            return stats;
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (String scope : queryService.scopes()) {
            rows.add(Map.of(
                    "key", scope,
                    "name", TextCleanUtil.cleanText(queryService.scopeDisplayName(scope)),
                    "minDate", "",
                    "maxDate", "",
                    "totalRows", 0,
                    "rowsOnTargetDate", 0
            ));
        }
        return rows;
    }

    @GetMapping("/branches")
    public List<String> branches(@RequestParam(name = "scope", required = false) String scope) {
        return queryService.branches(scope);
    }

    @GetMapping("/branches/debug")
    public Map<String, Object> branchDebug(@RequestParam(name = "scope", required = false) String scope) {
        return queryService.branchDiagnostics(scope);
    }

    @GetMapping("/metrics")
    public List<Map<String, Object>> metrics(@RequestParam(name = "scope", required = false) String scope) {
        boolean checkAvailability = scope != null && !scope.isBlank();
        return queryService.metrics().stream()
                .map(m -> {
                    boolean available = !checkAvailability || queryService.metricAvailable(scope, m.metric());
                    boolean calcMetric = m.metric() != null && m.metric().toUpperCase().startsWith("CALC_");
                    return Map.<String, Object>of(
                            "key", m.metric(),
                            "metric", m.metric(),
                            "name", TextCleanUtil.cleanText(m.name()),
                            "unit", TextCleanUtil.cleanText(m.unit()),
                            "kind", m.kind() == null ? "" : TextCleanUtil.cleanText(m.kind()),
                            "baseMetric", m.baseMetric() == null ? "" : TextCleanUtil.cleanText(m.baseMetric()),
                            "available", available,
                            "availability", available ? "可用" : "未生成",
                            "defaultSelected", available && !calcMetric
                    );
                })
                .toList();
    }

    @GetMapping("/meta")
    public Map<String, Object> meta() {
        return queryService.meta();
    }
}
