package com.example.loantrendhub.service;

import com.example.loantrendhub.model.*;
import com.example.loantrendhub.repo.FactRepo;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class QueryService {
    private static final double EPSILON = 1e-9;
    private final FactRepo factRepo;
    private final MetricService metricService;

    public QueryService(FactRepo factRepo, MetricService metricService) {
        this.factRepo = factRepo;
        this.metricService = metricService;
    }

    public List<String> scopes() { return factRepo.findScopes(); }
    public List<String> branches(String scope) { return factRepo.findBranches(scope); }
    public List<MetricDef> metrics() { return metricService.listMetrics(); }

    public HeatmapResponse heatmap(String scope, String date, List<String> metrics) {
        List<String> branches = branches(scope);
        Map<String, Integer> xIndex = new HashMap<>();
        for (int i = 0; i < branches.size(); i++) xIndex.put(branches.get(i), i);

        List<FactRow> rows = factRepo.findByDateScopeMetrics(date, scope, metrics);
        List<List<Object>> data = new ArrayList<>();
        for (FactRow row : rows) {
            Integer xi = xIndex.get(row.branch());
            int yi = metrics.indexOf(row.metric());
            if (xi == null || yi < 0 || row.value() == null) continue;
            data.add(List.of(xi, yi, row.value()));
        }

        Map<String, MetricDef> defs = metricService.metricMap();
        Map<String, HeatmapResponse.MetricMeta> metricDefs = metrics.stream().collect(Collectors.toMap(
                m -> m,
                m -> {
                    MetricDef d = defs.get(m);
                    return d == null ? new HeatmapResponse.MetricMeta(m, "") : new HeatmapResponse.MetricMeta(d.name(), d.unit());
                }
        ));

        return new HeatmapResponse(date, scope, branches, metrics, data, metricDefs);
    }

    public SeriesResponse multiTrend(String scope, String metric, List<String> branches, String branch, List<String> metrics, String start, String end) {
        if (metric != null && branches != null && !branches.isEmpty()) {
            return seriesByBranches(scope, metric, branches, start, end);
        }
        if (branch != null && metrics != null && !metrics.isEmpty()) {
            return seriesByMetrics(scope, branch, metrics, start, end);
        }
        throw new IllegalArgumentException("请传 metric+branches 或 branch+metrics");
    }

    public List<GrowthDef> calcGrowth(String scope, String branch, String metric, String start, String end) {
        List<FactRow> rows = factRepo.findSeries(scope, branch, metric, start, end);
        Map<String, Double> valueByDate = rows.stream().collect(Collectors.toMap(FactRow::bizDate, FactRow::value, (a, b) -> b));
        List<String> dates = new ArrayList<>(valueByDate.keySet());
        Collections.sort(dates);

        List<GrowthDef> out = new ArrayList<>();
        for (String dateStr : dates) {
            LocalDate date = LocalDate.parse(dateStr);
            Double cur = valueByDate.get(dateStr);
            Double prevDay = valueByDate.get(date.minusDays(1).toString());
            Double monthStart = valueByDate.get(date.withDayOfMonth(1).toString());
            Double yearStart = valueByDate.get(date.withDayOfYear(1).toString());

            Double dod = safeRate(cur, prevDay);
            Double mtd = safeRate(cur, monthStart);
            Double ytd = safeRate(cur, yearStart);
            String note = null;
            if (prevDay != null && prevDay == 0.0 && cur != null && cur != 0.0) note = "基期为0，新增";
            out.add(new GrowthDef(dateStr, cur, dod, mtd, ytd, note));
        }
        return out;
    }

    public Map<String, Object> exportReport(String scope, String date, List<String> metrics) {
        HeatmapResponse heatmap = heatmap(scope, date, metrics);
        return Map.of(
                "title", "贷款多视角日报分析",
                "scope", scope,
                "date", date,
                "definition", Map.of(
                        "delta", "增长量=本期值-对比期值",
                        "growthRate", "增长率=增长量/对比期值；基期为0时不定义，显示新增",
                        "contribution", "贡献度=网点增长量/全行增长量"
                ),
                "heatmap", heatmap
        );
    }

    private SeriesResponse seriesByBranches(String scope, String metric, List<String> branches, String start, String end) {
        List<String> x = factRepo.findDates(scope, start, end);
        List<FactRow> rows = factRepo.findSeriesByMetric(scope, metric, branches, start, end);
        Map<String, Map<String, Double>> dateBranchVal = new HashMap<>();
        for (FactRow row : rows) {
            dateBranchVal.computeIfAbsent(row.bizDate(), k -> new HashMap<>()).put(row.branch(), row.value());
        }
        List<SeriesResponse.SeriesItem> series = new ArrayList<>();
        for (String b : branches) {
            List<Double> y = x.stream().map(d -> dateBranchVal.getOrDefault(d, Map.of()).get(b)).collect(Collectors.toList());
            series.add(new SeriesResponse.SeriesItem(b, y));
        }
        MetricDef def = metricService.metricMap().get(metric);
        Map<String, Object> metricObj = def == null ? Map.of("key", metric, "name", metric) :
                Map.of("key", def.metric(), "name", def.name(), "unit", def.unit());
        return new SeriesResponse(x, series, metricObj);
    }

    private SeriesResponse seriesByMetrics(String scope, String branch, List<String> metrics, String start, String end) {
        List<String> x = factRepo.findDates(scope, start, end);
        List<FactRow> rows = factRepo.findSeriesByBranch(scope, branch, metrics, start, end);
        Map<String, Map<String, Double>> dateMetricVal = new HashMap<>();
        for (FactRow row : rows) {
            dateMetricVal.computeIfAbsent(row.bizDate(), k -> new HashMap<>()).put(row.metric(), row.value());
        }

        Map<String, MetricDef> metricMap = metricService.metricMap();
        List<SeriesResponse.SeriesItem> series = new ArrayList<>();
        for (String m : metrics) {
            String name = metricMap.containsKey(m) ? metricMap.get(m).name() + "(" + m + ")" : m;
            List<Double> y = x.stream().map(d -> dateMetricVal.getOrDefault(d, Map.of()).get(m)).collect(Collectors.toList());
            series.add(new SeriesResponse.SeriesItem(name, y));
        }
        return new SeriesResponse(x, series, Map.of("branch", branch));
    }

    private Double safeRate(Double cur, Double base) {
        if (cur == null || base == null) return null;
        if (Math.abs(base) < EPSILON) {
            if (Math.abs(cur) < EPSILON) return 0.0;
            return null;
        }
        return (cur - base) / Math.max(Math.abs(base), EPSILON);
    }
}