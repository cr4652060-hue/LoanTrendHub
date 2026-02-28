package com.example.loantrendhub.controller;

import com.example.loantrendhub.model.HeatmapResponse;
import com.example.loantrendhub.model.SeriesResponse;
import com.example.loantrendhub.service.QueryService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class QueryController {
    private final QueryService queryService;

    public QueryController(QueryService queryService) {
        this.queryService = queryService;
    }

    /** 前端初始化：自动带出库内已有数据日期区间 */
    @GetMapping("/dateRange")
    public Object dateRange() {
        return queryService.dateRange();
    }

    @GetMapping("/heatmap")
    public HeatmapResponse heatmap(
            @RequestParam(name = "scope") String scope,
            @RequestParam(name = "date") String date,
            @RequestParam(name = "metrics") String metrics
    ) {
        return queryService.heatmap(scope, date, splitCsv(metrics));
    }

    /** 正式接口：多网点/多指标趋势 */
    @GetMapping("/trend/multi")
    public SeriesResponse multiTrend(
            @RequestParam(name = "scope") String scope,
            @RequestParam(name = "metric", required = false) String metric,
            @RequestParam(name = "branches", required = false) String branches,
            @RequestParam(name = "branch", required = false) String branch,
            @RequestParam(name = "metrics", required = false) String metrics,
            @RequestParam(name = "start") String start,
            @RequestParam(name = "end") String end
    ) {
        List<String> branchList = branches == null ? List.of() : splitCsv(branches);
        List<String> metricList = metrics == null ? List.of() : splitCsv(metrics);
        return queryService.multiTrend(scope, metric, branchList, branch, metricList, start, end);
    }

    /** ✅ 兼容旧前端写错的路径：/api/trendMulti */
    @GetMapping("/trendMulti")
    public SeriesResponse trendMultiCompat(
            @RequestParam(name = "scope") String scope,
            @RequestParam(name = "metric") String metric,
            @RequestParam(name = "branches", required = false) String branches,
            @RequestParam(name = "start") String start,
            @RequestParam(name = "end") String end
    ) {
        List<String> branchList = branches == null ? List.of() : splitCsv(branches);
        return queryService.multiTrend(scope, metric, branchList, null, List.of(), start, end);
    }

    /** ✅ 增长率（前端口径：deltaMetric/baseMetric + branches） */
    @GetMapping("/growth")
    public SeriesResponse growth(
            @RequestParam(name = "scope") String scope,
            @RequestParam(name = "deltaMetric") String deltaMetric,
            @RequestParam(name = "baseMetric") String baseMetric,
            @RequestParam(name = "start") String start,
            @RequestParam(name = "end") String end,
            @RequestParam(name = "branches", required = false) String branches
    ) {
        List<String> branchList = branches == null ? List.of() : splitCsv(branches);
        return queryService.growthSeries(scope, deltaMetric, baseMetric, branchList, start, end);
    }

    @GetMapping("/report/export")
    public ResponseEntity<?> export(
            @RequestParam(name = "scope") String scope,
            @RequestParam(name = "date") String date,
            @RequestParam(name = "metrics") String metrics,
            @RequestParam(name = "format", defaultValue = "json") String format
    ) {
        Map<String, Object> payload = queryService.exportReport(scope, date, splitCsv(metrics));
        if ("html".equalsIgnoreCase(format)) {
            String html = "<html><head><meta charset='UTF-8'><title>LoanTrendHub Report</title></head>" +
                    "<body><h1>贷款日报分析报告</h1><pre>" + payload + "</pre></body></html>";
            return ResponseEntity.ok()
                    .contentType(MediaType.TEXT_HTML)
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=loan_report.html")
                    .body(html);
        }
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=loan_report.json")
                .body(payload);
    }

    private List<String> splitCsv(String s) {
        return Arrays.stream(s.split(","))
                .map(String::trim)
                .filter(v -> !v.isBlank())
                .toList();
    }
}
