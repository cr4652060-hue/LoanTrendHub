package com.example.loantrendhub.controller;

import com.example.loantrendhub.model.MetricDef;
import com.example.loantrendhub.service.QueryService;
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
        return queryService.scopes().stream()
                .map(s -> Map.of("key", s, "name", switch (s) {
                    case "PHY" -> "实体贷款（纯账面）";
                    case "ADJ" -> "实体贷款（还原剔转）";
                    default -> s;
                }))
                .toList();
    }

    @GetMapping("/branches")
    public List<String> branches(@RequestParam(name = "scope") String scope) {
        return queryService.branches(scope);
    }

    @GetMapping("/metrics")
    public List<MetricDef> metrics() {
        return queryService.metrics();
    }
}
