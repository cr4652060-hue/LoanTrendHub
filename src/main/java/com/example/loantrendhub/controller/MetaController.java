package com.example.loantrendhub.controller;

import com.example.loantrendhub.model.MetricDef;
import com.example.loantrendhub.service.QueryService;
import com.example.loantrendhub.util.DateUtil;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
public List<Map<String, String>> scopes() {
    return List.of(
            Map.of("key", "PHY", "name", "实体贷款（纯账面）"),
            Map.of("key", "ADJ", "name", "实体贷款（还原剔转）")
    );
}

    @GetMapping("/branches")
    public List<String> branches(@RequestParam(name = "scope") String scope) {
        return queryService.branches(scope);
    }

    @GetMapping("/metrics")
public List<Map<String, Object>> metrics() {
    return queryService.metrics().stream()
            .map(m -> Map.<String, Object>of(
                    "key", m.metric(),
                    "name", m.name(),
                    "unit", m.unit(),
                    "kind", m.kind() == null ? "" : m.kind(),
                    "baseMetric", m.baseMetric() == null ? "" : m.baseMetric()
            ))
            .toList();
}
}
