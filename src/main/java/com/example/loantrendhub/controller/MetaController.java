package com.example.loantrendhub.controller;

import com.example.loantrendhub.service.QueryService;
import com.example.loantrendhub.util.DateUtil;
import com.example.loantrendhub.util.TextCleanUtil;
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
        List<String> availableScopes = queryService.scopes();
        if (availableScopes.isEmpty()) {
            availableScopes = List.of("PHY", "ADJ");
        }
        return availableScopes.stream()
                .map(DateUtil::normalizeScope)
                .distinct()
                .map(scope -> Map.of(
                        "key", scope,
                        "name", TextCleanUtil.cleanText(DateUtil.scopeDisplayName(scope))
                ))
                .toList();
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
    public List<Map<String, Object>> metrics() {
        return queryService.metrics().stream()
                .map(m -> Map.<String, Object>of(
                        "key", m.metric(),
                        "name", TextCleanUtil.cleanText(m.name()),
                        "unit", TextCleanUtil.cleanText(m.unit()),
                        "kind", m.kind() == null ? "" : TextCleanUtil.cleanText(m.kind()),
                        "baseMetric", m.baseMetric() == null ? "" : TextCleanUtil.cleanText(m.baseMetric())
                ))
                .toList();
    }
    @GetMapping("/meta")
    public Map<String, Object> meta() {
        return queryService.meta();
    }
}
