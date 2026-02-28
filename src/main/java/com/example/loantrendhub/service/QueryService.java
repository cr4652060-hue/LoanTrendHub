package com.example.loantrendhub.service;

import com.example.loantrendhub.model.*;
import com.example.loantrendhub.repo.FactRepo;
import com.example.loantrendhub.util.ScopeUtil;
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

    public Map<String, String> dateRange() {
        return factRepo.dateRange();
    }

    public List<String> scopes() { return factRepo.findScopes(); }

    public List<String> branches(String scope) { return factRepo.findBranches(ScopeUtil.normalize(scope)); }

    public List<MetricDef> metrics() { return metricService.listMetrics(); }

    public HeatmapResponse heatmap(String scope, String date, List<String> metrics) {
        scope = ScopeUtil.normalize(scope);
        List<String> branches = branches(scope);

        Map<String, Integer> metricIdx = new HashMap<>();
        for (int i = 0; i < metrics.size(); i++) metricIdx.put(metrics.get(i), i);

        Map<String, Integer> branchIdx = new HashMap<>();
        for (int i = 0; i < branches.size(); i++) branchIdx.put(branches.get(i), i);

        List<FactRow> rows = factRepo.findByDateScopeMetrics(date, scope, metrics);
        List<List<Object>> data = new ArrayList<>();
        Double min = null, max = null;

        for (FactRow row : rows) {
            Integer xi = metricIdx.get(row.metric());
            Integer yi = branchIdx.get(row.branch());
            if (xi == null || yi == null || row.value() == null) continue;

            double v = row.value();
            data.add(List.of(xi, yi, v));
            min = (min == null) ? v : Math.min(min, v);
            max = (max == null) ? v : Math.max(max, v);
        }

        if (min == null) min = 0d;
        if (max == null) max = 1d;

        Map<String, MetricDef> defs = metricService.metricMap();
        Map<String, HeatmapResponse.MetricMeta> metricDefs = metrics.stream().collect(Collectors.toMap(
                m -> m,
                m -> {
                    MetricDef d = defs.get(m);
                    return d == null ? new HeatmapResponse.MetricMeta(m, "") : new HeatmapResponse.MetricMeta(d.name(), d.unit());
                }
        ));

        return new HeatmapResponse(date, scope, metrics, branches, min, max, data, metricDefs);
    }

    /** 统一：同指标多网点 或 同网点多指标 */
    public SeriesResponse multiTrend(String scope,
                                     String metric,
                                     List<String> branches,
                                     String branch,
                                     List<String> metrics,
                                     String start,
                                     String end) {
        scope = ScopeUtil.normalize(scope);
        if (metric != null && !metric.isBlank() && branches != null && !branches.isEmpty()) {
            return seriesByBranches(scope, metric, branches, start, end);
        }
        if (branch != null && !branch.isBlank() && metrics != null && !metrics.isEmpty()) {
            return seriesByMetrics(scope, branch, metrics, start, end);
        }
        return new SeriesResponse("趋势：" + scope, "", List.of(), List.of());
    }

    private SeriesResponse seriesByBranches(String scope, String metric, List<String> branches, String start, String end) {
        List<String> dates = factRepo.findDates(scope, start, end);
        List<FactRow> rows = factRepo.findSeriesByMetric(scope, metric, branches, start, end);

        Map<String, Map<String, Double>> byBranch = new LinkedHashMap<>();
        for (String b : branches) byBranch.put(b, new HashMap<>());
        for (FactRow r : rows) {
            if (r.value() == null) continue;
            byBranch.computeIfAbsent(r.branch(), k -> new HashMap<>()).put(r.bizDate(), r.value());
        }

        List<SeriesResponse.Series> series = new ArrayList<>();
        for (String b : branches) {
            Map<String, Double> map = byBranch.getOrDefault(b, Map.of());
            List<Double> y = dates.stream().map(d -> map.getOrDefault(d, null)).toList();
            series.add(new SeriesResponse.Series(b, y));
        }

        MetricDef md = metricService.metricMap().get(metric);
        String unit = md == null ? "" : md.unit();
        return new SeriesResponse("趋势：" + scope + " / " + (md == null ? metric : md.name()), unit, dates, series);
    }

    private SeriesResponse seriesByMetrics(String scope, String branch, List<String> metrics, String start, String end) {
        List<String> dates = factRepo.findDates(scope, start, end);
        List<FactRow> rows = factRepo.findSeriesByBranch(scope, branch, metrics, start, end);

        Map<String, Map<String, Double>> byMetric = new LinkedHashMap<>();
        for (String m : metrics) byMetric.put(m, new HashMap<>());
        for (FactRow r : rows) {
            if (r.value() == null) continue;
            byMetric.computeIfAbsent(r.metric(), k -> new HashMap<>()).put(r.bizDate(), r.value());
        }

        Map<String, MetricDef> mdMap = metricService.metricMap();
        List<SeriesResponse.Series> series = new ArrayList<>();
        for (String m : metrics) {
            Map<String, Double> map = byMetric.getOrDefault(m, Map.of());
            List<Double> y = dates.stream().map(d -> map.getOrDefault(d, null)).toList();
            MetricDef md = mdMap.get(m);
            series.add(new SeriesResponse.Series(md == null ? m : md.name(), y));
        }

        return new SeriesResponse("趋势：" + scope + " / " + branch, "", dates, series);
    }

    /**
     * ✅ 前端增长率口径：growth(t) = deltaMetric(t) / baseMetric(t-1) * 100
     * - 分母为 0/null -> 返回 null（前端显示 -）
     */
    public SeriesResponse growthSeries(String scope,
                                       String deltaMetric,
                                       String baseMetric,
                                       List<String> branches,
                                       String start,
                                       String end) {
        scope = ScopeUtil.normalize(scope);
        List<String> dates = factRepo.findDates(scope, start, end);
        if (dates.isEmpty()) return new SeriesResponse("增长率：" + scope, "%", List.of(), List.of());

        // 拉取两条指标的明细（同一时间窗口）
        List<FactRow> deltaRows = factRepo.findSeriesByMetric(scope, deltaMetric, branches, start, end);
        List<FactRow> baseRows  = factRepo.findSeriesByMetric(scope, baseMetric,  branches, start, end);

        Map<String, Map<String, Double>> delta = new HashMap<>();
        for (FactRow r : deltaRows) {
            if (r.value() == null) continue;
            delta.computeIfAbsent(r.branch(), k -> new HashMap<>()).put(r.bizDate(), r.value());
        }
        Map<String, Map<String, Double>> base = new HashMap<>();
        for (FactRow r : baseRows) {
            if (r.value() == null) continue;
            base.computeIfAbsent(r.branch(), k -> new HashMap<>()).put(r.bizDate(), r.value());
        }

        List<SeriesResponse.Series> series = new ArrayList<>();
        for (String b : branches) {
            Map<String, Double> dmap = delta.getOrDefault(b, Map.of());
            Map<String, Double> bmap = base.getOrDefault(b, Map.of());

            List<Double> y = new ArrayList<>(dates.size());
            for (int i = 0; i < dates.size(); i++) {
                if (i == 0) { y.add(null); continue; }
                String d = dates.get(i);
                String prev = dates.get(i - 1);
                Double dv = dmap.get(d);
                Double bv = bmap.get(prev); // 上期存量
                if (dv == null || bv == null || Math.abs(bv) < EPSILON) y.add(null);
                else y.add(dv / bv * 100.0);
            }
            series.add(new SeriesResponse.Series(b, y));
        }

        MetricDef d1 = metricService.metricMap().get(deltaMetric);
        MetricDef d2 = metricService.metricMap().get(baseMetric);
        String t1 = d1 == null ? deltaMetric : d1.name();
        String t2 = d2 == null ? baseMetric : d2.name();

        return new SeriesResponse("增长率：" + scope + "（" + t1 + " ÷ 上期" + t2 + "）", "%", dates, series);
    }

    public Map<String, Object> exportReport(String scope, String date, List<String> metrics) {
        scope = ScopeUtil.normalize(scope);
        HeatmapResponse hm = heatmap(scope, date, metrics);
        return Map.of(
                "title", "贷款多视角日报分析",
                "date", date,
                "scope", scope,
                "metrics", metrics,
                "branches", hm.branches(),
                "heatmapData", hm.data(),
                "min", hm.min(),
                "max", hm.max()
        );
    }
}
