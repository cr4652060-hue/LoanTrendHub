package com.example.loantrendhub.service;

import com.example.loantrendhub.model.FactRow;
import com.example.loantrendhub.model.HeatmapResponse;
import com.example.loantrendhub.model.MetricDef;
import com.example.loantrendhub.model.SeriesResponse;
import com.example.loantrendhub.repo.FactRepo;
import com.example.loantrendhub.util.BranchNormalizeUtil;
import com.example.loantrendhub.util.BranchNormalizer;
import com.example.loantrendhub.util.DateUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Service
public class QueryService {

    private static final double EPSILON = 1e-9;
    private final FactRepo factRepo;
    private final MetricService metricService;
    private final int maxBranchSeries;
    private final int maxPoints;

    public QueryService(FactRepo factRepo,
                        MetricService metricService,
                        @Value("${app.query.max-branch-series:200}") int maxBranchSeries,
                        @Value("${app.query.max-points:200000}") int maxPoints) {
        this.factRepo = factRepo;
        this.metricService = metricService;
        this.maxBranchSeries = maxBranchSeries;
        this.maxPoints = maxPoints;
    }

    private void ensureMetadataReady() {
        int branchCount = factRepo.countEnabledBranches();
        int metricCount = factRepo.countMetricDefs();
        if (branchCount <= 0 || metricCount <= 0) {
            throw new MetadataNotReadyException("元数据未初始化，请检查 schema-mysql.sql 是否已执行");
        }
    }

    public Map<String, Object> dateRange() {
        ensureMetadataReady();
        Map<String, String> raw = factRepo.dateRange();
        String min = raw.getOrDefault("min", "");
        String max = raw.getOrDefault("max", "");
        boolean hasData = min != null && !min.isBlank() && max != null && !max.isBlank();
        return Map.of(
                "min", hasData ? min : "",
                "max", hasData ? max : "",
                "hasData", hasData
        );
    }

    public List<String> scopes() {
        ensureMetadataReady();
        return factRepo.findScopes();
    }

    public List<Map<String, Object>> scopeStats(String targetDate) {
        ensureMetadataReady();
        List<FactRepo.ScopeStat> stats = factRepo.findScopeStats(targetDate);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (FactRepo.ScopeStat stat : stats) {
            rows.add(Map.of(
                    "key", stat.scope(),
                    "name", scopeDisplayName(stat.scope()),
                    "minDate", stat.minDate() == null ? "" : stat.minDate(),
                    "maxDate", stat.maxDate() == null ? "" : stat.maxDate(),
                    "totalRows", stat.totalRows(),
                    "rowsOnTargetDate", stat.rowsOnTargetDate()
            ));
        }
        return rows;
    }

    public List<String> branches(String scope) {
        ensureMetadataReady();
        String resolvedScope = resolveScope(scope);
        return factRepo.findBranches(resolvedScope).stream()
                .map(BranchNormalizeUtil::normalizeDisplay)
                .filter(v -> v != null && !v.isBlank())
                .distinct()
                .toList();
    }

    public Map<String, Object> branchDiagnostics(String scope) {
        return factRepo.branchDiagnostics(resolveScope(scope));
    }

    public List<MetricDef> metrics() {
        ensureMetadataReady();
        return metricService.listMetrics();
    }

    public boolean metricAvailable(String scope, String metric) {
        String resolvedScope = resolveScope(scope);
        return factRepo.metricExistsInScope(resolvedScope, metric);
    }

    public Map<String, Object> meta() {
        ensureMetadataReady();
        Map<String, Object> dateRange = dateRange();
        String maxDate = String.valueOf(dateRange.getOrDefault("max", ""));

        List<String> scopes = scopes();
        List<String> cleanedBranches = factRepo.findAllBranches();
        List<Map<String, Object>> scopeStats = scopeStats(maxDate);

        String suggestedScope = scopes.isEmpty() ? "" : scopes.get(0);
        for (Map<String, Object> stat : scopeStats) {
            long rows = ((Number) stat.getOrDefault("rowsOnTargetDate", 0L)).longValue();
            if (rows > 0) {
                suggestedScope = String.valueOf(stat.get("key"));
                break;
            }
        }

        return Map.of(
                "scopes", scopes,
                "scopeStats", scopeStats,
                "suggestedScope", suggestedScope,
                "metrics", metrics(),
                "branches", cleanedBranches,
                "dateRange", dateRange
        );
    }

    public HeatmapResponse heatmap(String scope, String date, List<String> metrics) {
        String resolvedScope = resolveScope(scope);
        List<String> selectedMetrics = (metrics == null ? List.<String>of() : metrics).stream()
                .map(v -> v == null ? "" : v.trim())
                .filter(v -> !v.isBlank())
                .distinct()
                .toList();

        List<String> branches = branches(resolvedScope);
        Map<String, MetricDef> defs = metricService.metricMap();

        if (selectedMetrics.isEmpty()) {
            return new HeatmapResponse(
                    date,
                    resolvedScope,
                    List.of(),
                    branches,
                    0d,
                    1d,
                    List.of(),
                    List.of(),
                    Map.of(),
                    new HeatmapResponse.Meta(date, resolvedScope, "", 0, true)
            );
        }

        Map<String, Integer> metricIdx = new HashMap<>();
        for (int i = 0; i < selectedMetrics.size(); i++) {
            metricIdx.put(selectedMetrics.get(i), i);
        }

        Map<String, Integer> branchIdx = new HashMap<>();
        for (int i = 0; i < branches.size(); i++) {
            branchIdx.put(branches.get(i), i);
        }

        List<FactRepo.HeatmapCell> source = factRepo.findHeatmapMatrix(date, resolvedScope, selectedMetrics);
        List<List<Object>> data = new ArrayList<>();
        List<HeatmapResponse.Cell> cells = new ArrayList<>();
        Double min = null;
        Double max = null;
        int dataRowCount = 0;

        for (FactRepo.HeatmapCell row : source) {
            Integer xi = metricIdx.get(row.metric());
            Integer yi = branchIdx.get(row.branch());
            if (xi == null || yi == null) {
                continue;
            }
            boolean hasData = row.val() != null;
            data.add(List.of(xi, yi, row.val(), hasData));
            cells.add(new HeatmapResponse.Cell(row.branch(), row.metric(), row.val(), hasData));
            if (!hasData) {
                continue;
            }
            double v = row.val();
            dataRowCount++;
            min = (min == null) ? v : Math.min(min, v);
            max = (max == null) ? v : Math.max(max, v);
        }

        boolean allNull = dataRowCount == 0;
        if (min == null) {
            min = 0d;
        }
        if (max == null) {
            max = 1d;
        }
        if (Double.compare(min, max) == 0) {
            max = min + 1d;
        }

        Map<String, HeatmapResponse.MetricMeta> metricDefs = new LinkedHashMap<>();
        for (String metric : selectedMetrics) {
            MetricDef d = defs.get(metric);
            metricDefs.put(metric, d == null ? new HeatmapResponse.MetricMeta(metric, "") : new HeatmapResponse.MetricMeta(d.name(), d.unit()));
        }

        return new HeatmapResponse(
                date,
                resolvedScope,
                selectedMetrics,
                branches,
                min,
                max,
                data,
                cells,
                metricDefs,
                new HeatmapResponse.Meta(
                        date,
                        resolvedScope,
                        String.join(",", selectedMetrics),
                        dataRowCount,
                        allNull
                )
        );
    }

    /** 统一：同指标多网点，或同网点多指标 */
    public SeriesResponse multiTrend(String scope,
                                     String metric,
                                     List<String> branches,
                                     String branch,
                                     List<String> metrics,
                                     String start,
                                     String end) {
        String resolvedScope = resolveScope(scope);
        if (metric != null && !metric.trim().isEmpty() && branches != null && !branches.isEmpty()) {
            return seriesByBranches(resolvedScope, metric, branches, start, end);
        }
        if (branch != null && !branch.trim().isEmpty() && metrics != null && !metrics.isEmpty()) {
            return seriesByMetrics(resolvedScope, branch, metrics, start, end);
        }
        return new SeriesResponse("趋势: " + resolvedScope, "", List.of(), List.of(), List.of(),
                new SeriesResponse.Meta(0, 0, false, 0, start + " ~ " + end, resolvedScope, "", 0, true));
    }

    private SeriesResponse seriesByBranches(String scope, String metric, List<String> branches, String start, String end) {
        List<String> warnings = new ArrayList<>();
        List<String> resolvedBranches = normalizeAndResolveBranches(scope, branches, warnings);
        int requested = resolvedBranches.size();

        if (maxBranchSeries > 0 && resolvedBranches.size() > maxBranchSeries) {
            int dropped = resolvedBranches.size() - maxBranchSeries;
            resolvedBranches = resolvedBranches.subList(0, maxBranchSeries);
            warnings.add("网点数量超过上限，已按配置截断到 " + maxBranchSeries + " 个。已忽略 " + dropped + " 个网点。");
        }

        List<String> dates = new ArrayList<>(new TreeSet<>(factRepo.findDates(scope, start, end)));
        List<FactRow> rows = factRepo.findSeriesByMetric(scope, metric, resolvedBranches, start, end);
        MetricDef md = metricService.metricMap().get(metric);
        if (rows.isEmpty() && md != null && "DELTA".equalsIgnoreCase(md.kind()) && md.baseMetric() != null && !md.baseMetric().isBlank()) {
            rows = buildDeltaRowsFromBase(scope, resolvedBranches, dates, metric, md.baseMetric(), start, end);
        }
        if (rows.isEmpty() && md != null && "RATE".equalsIgnoreCase(md.kind()) && md.baseMetric() != null && !md.baseMetric().isBlank()) {
            rows = buildRateRowsFromBase(scope, resolvedBranches, dates, metric, md.baseMetric(), start, end);
        }

        DegradedWindow degraded = degradeRowsIfNeeded(dates, resolvedBranches, rows, warnings, 1);
        dates = degraded.dates();
        resolvedBranches = degraded.branches();
        rows = degraded.rows();

        Map<String, Map<String, Double>> byBranch = new LinkedHashMap<>();
        for (String b : resolvedBranches) {
            byBranch.put(b, new HashMap<>());
        }
        for (FactRow r : rows) {
            if (r.val() == null) {
                continue;
            }
            byBranch.computeIfAbsent(r.branch(), k -> new HashMap<>()).put(r.bizDate(), r.val());
        }

        int dataRowCount = 0;
        List<SeriesResponse.Series> series = new ArrayList<>();
        for (String b : resolvedBranches) {
            Map<String, Double> map = byBranch.getOrDefault(b, Map.of());
            List<Double> y = new ArrayList<>(dates.size());
            for (String d : dates) {
                Double v = map.get(d);
                Double point = (v == null || !Double.isFinite(v)) ? null : v;
                if (point != null) {
                    dataRowCount++;
                }
                y.add(point);
            }
            series.add(new SeriesResponse.Series(b, y));
        }

        String unit = md == null ? "" : md.unit();
        return new SeriesResponse(
                "趋势: " + scope + " / " + (md == null ? metric : md.name()),
                unit,
                dates,
                series,
                warnings,
                new SeriesResponse.Meta(
                        requested,
                        resolvedBranches.size(),
                        requested != resolvedBranches.size(),
                        Math.max(0, requested - resolvedBranches.size()),
                        start + " ~ " + end,
                        scope,
                        metric,
                        dataRowCount,
                        dataRowCount == 0
                )
        );
    }

    private List<FactRow> buildDeltaRowsFromBase(String scope,
                                                 List<String> branches,
                                                 List<String> dates,
                                                 String deltaMetric,
                                                 String baseMetric,
                                                 String start,
                                                 String end) {
        if (branches == null || branches.isEmpty() || dates == null || dates.isEmpty()) {
            return List.of();
        }
        List<FactRow> baseRows = factRepo.findSeriesByMetric(scope, baseMetric, branches, start, end);
        Map<String, Map<String, Double>> byBranch = new HashMap<>();
        for (FactRow row : baseRows) {
            if (row.val() == null) {
                continue;
            }
            byBranch.computeIfAbsent(row.branch(), k -> new HashMap<>()).put(row.bizDate(), row.val());
        }

        List<FactRow> deltaRows = new ArrayList<>();
        for (String branch : branches) {
            Map<String, Double> values = byBranch.getOrDefault(branch, Map.of());
            for (int i = 1; i < dates.size(); i++) {
                String currDate = dates.get(i);
                String prevDate = dates.get(i - 1);
                Double curr = values.get(currDate);
                Double prev = values.get(prevDate);
                if (curr == null || prev == null) {
                    continue;
                }
                deltaRows.add(new FactRow(currDate, scope, branch, deltaMetric, curr - prev, "AUTO_COMPUTED_DELTA"));
            }
        }
        return deltaRows;
    }

    private List<FactRow> buildRateRowsFromBase(String scope,
                                                List<String> branches,
                                                List<String> dates,
                                                String rateMetric,
                                                String baseMetric,
                                                String start,
                                                String end) {
        if (branches == null || branches.isEmpty() || dates == null || dates.isEmpty()) {
            return List.of();
        }
        List<FactRow> baseRows = factRepo.findSeriesByMetric(scope, baseMetric, branches, start, end);
        Map<String, Map<String, Double>> byBranch = new HashMap<>();
        for (FactRow row : baseRows) {
            if (row.val() == null) {
                continue;
            }
            byBranch.computeIfAbsent(row.branch(), k -> new HashMap<>()).put(row.bizDate(), row.val());
        }

        String metricUpper = String.valueOf(rateMetric).toUpperCase(Locale.ROOT);
        List<FactRow> rateRows = new ArrayList<>();
        for (String branch : branches) {
            Map<String, Double> values = byBranch.getOrDefault(branch, Map.of());
            for (int i = 1; i < dates.size(); i++) {
                String currDate = dates.get(i);
                String prevDate = dates.get(i - 1);
                Double curr = values.get(currDate);
                Double prev = values.get(prevDate);
                if (curr == null || prev == null || Math.abs(prev) < EPSILON) {
                    continue;
                }
                double ratio;
                if (metricUpper.contains("MTD")) {
                    ratio = calcMonthToDateRatio(values, dates, i, curr);
                } else if (metricUpper.contains("YTD")) {
                    ratio = calcYearToDateRatio(values, dates, i, curr);
                } else {
                    ratio = (curr - prev) / prev;
                }
                if (Double.isFinite(ratio)) {
                    rateRows.add(new FactRow(currDate, scope, branch, rateMetric, ratio * 100.0, "AUTO_COMPUTED_RATE"));
                }
            }
        }
        return rateRows;
    }

    private double calcMonthToDateRatio(Map<String, Double> values, List<String> dates, int idx, Double curr) {
        String currDate = dates.get(idx);
        LocalDate d = LocalDate.parse(currDate);
        LocalDate monthStart = d.withDayOfMonth(1);
        Double baseline = null;
        for (String day : dates) {
            LocalDate ld = LocalDate.parse(day);
            if (ld.isBefore(monthStart) || ld.isAfter(d)) {
                continue;
            }
            baseline = values.get(day);
            if (baseline != null) {
                break;
            }
        }
        if (baseline == null || Math.abs(baseline) < EPSILON) {
            return Double.NaN;
        }
        return (curr - baseline) / baseline;
    }

    private double calcYearToDateRatio(Map<String, Double> values, List<String> dates, int idx, Double curr) {
        String currDate = dates.get(idx);
        LocalDate d = LocalDate.parse(currDate);
        LocalDate yearStart = d.withDayOfYear(1);
        Double baseline = null;
        for (String day : dates) {
            LocalDate ld = LocalDate.parse(day);
            if (ld.isBefore(yearStart) || ld.isAfter(d)) {
                continue;
            }
            baseline = values.get(day);
            if (baseline != null) {
                break;
            }
        }
        if (baseline == null || Math.abs(baseline) < EPSILON) {
            return Double.NaN;
        }
        return (curr - baseline) / baseline;
    }

    private SeriesResponse seriesByMetrics(String scope, String branch, List<String> metrics, String start, String end) {
        List<String> selectedMetrics = (metrics == null ? List.<String>of() : metrics).stream()
                .map(v -> v == null ? "" : v.trim())
                .filter(v -> !v.isBlank())
                .distinct()
                .toList();

        List<String> dates = new ArrayList<>(new TreeSet<>(factRepo.findDates(scope, start, end)));
        List<FactRow> rows = factRepo.findSeriesByBranch(scope, branch, selectedMetrics, start, end);

        Map<String, Map<String, Double>> byMetric = new LinkedHashMap<>();
        for (String m : selectedMetrics) {
            byMetric.put(m, new HashMap<>());
        }
        for (FactRow r : rows) {
            if (r.val() == null) {
                continue;
            }
            byMetric.computeIfAbsent(r.metric(), k -> new HashMap<>()).put(r.bizDate(), r.val());
        }

        Map<String, MetricDef> mdMap = metricService.metricMap();
        int dataRowCount = 0;
        List<SeriesResponse.Series> series = new ArrayList<>();
        for (String m : selectedMetrics) {
            Map<String, Double> map = byMetric.getOrDefault(m, Map.of());
            List<Double> y = new ArrayList<>(dates.size());
            for (String d : dates) {
                Double v = map.get(d);
                Double point = (v == null || !Double.isFinite(v)) ? null : v;
                if (point != null) {
                    dataRowCount++;
                }
                y.add(point);
            }
            MetricDef md = mdMap.get(m);
            series.add(new SeriesResponse.Series(md == null ? m : md.name(), y));
        }

        return new SeriesResponse(
                "趋势: " + scope + " / " + branch,
                "",
                dates,
                series,
                List.of(),
                new SeriesResponse.Meta(
                        selectedMetrics.size(),
                        selectedMetrics.size(),
                        false,
                        0,
                        start + " ~ " + end,
                        scope,
                        String.join(",", selectedMetrics),
                        dataRowCount,
                        dataRowCount == 0
                )
        );
    }

    public SeriesResponse growthSeries(String scope,
                                       String deltaMetric,
                                       String baseMetric,
                                       List<String> branches,
                                       String start,
                                       String end) {
        String resolvedScope = resolveScope(scope);
        List<String> warnings = new ArrayList<>();
        List<String> resolvedBranches = normalizeAndResolveBranches(resolvedScope, branches, warnings);
        int requested = resolvedBranches.size();

        if (maxBranchSeries > 0 && resolvedBranches.size() > maxBranchSeries) {
            int dropped = resolvedBranches.size() - maxBranchSeries;
            resolvedBranches = resolvedBranches.subList(0, maxBranchSeries);
            warnings.add("网点数量超过上限，已按配置截断到 " + maxBranchSeries + " 个。已忽略 " + dropped + " 个网点。");
        }

        List<String> dates = new ArrayList<>(new TreeSet<>(factRepo.findDates(resolvedScope, start, end)));
        if (dates.isEmpty()) {
            return new SeriesResponse(
                    "增长率: " + resolvedScope,
                    "%",
                    List.of(),
                    List.of(),
                    warnings,
                    new SeriesResponse.Meta(
                            requested,
                            resolvedBranches.size(),
                            requested != resolvedBranches.size(),
                            Math.max(0, requested - resolvedBranches.size()),
                            start + " ~ " + end,
                            resolvedScope,
                            deltaMetric + "/" + baseMetric,
                            0,
                            true
                    )
            );
        }

        List<FactRow> deltaRows = factRepo.findSeriesByMetric(resolvedScope, deltaMetric, resolvedBranches, start, end);
        List<FactRow> baseRows = factRepo.findSeriesByMetric(resolvedScope, baseMetric, resolvedBranches, start, end);
        MetricDef deltaDef = metricService.metricMap().get(deltaMetric);
        if (deltaRows.isEmpty() && deltaDef != null && "DELTA".equalsIgnoreCase(deltaDef.kind()) && deltaDef.baseMetric() != null && !deltaDef.baseMetric().isBlank()) {
            deltaRows = buildDeltaRowsFromBase(resolvedScope, resolvedBranches, dates, deltaMetric, deltaDef.baseMetric(), start, end);
        }

        Map<String, Map<String, Double>> delta = new HashMap<>();
        for (FactRow r : deltaRows) {
            if (r.val() == null) {
                continue;
            }
            delta.computeIfAbsent(r.branch(), k -> new HashMap<>()).put(r.bizDate(), r.val());
        }

        Map<String, Map<String, Double>> base = new HashMap<>();
        for (FactRow r : baseRows) {
            if (r.val() == null) {
                continue;
            }
            base.computeIfAbsent(r.branch(), k -> new HashMap<>()).put(r.bizDate(), r.val());
        }

        List<SeriesResponse.Series> series = new ArrayList<>();
        DegradedWindow growthWindow = degradeRowsIfNeeded(dates, resolvedBranches, deltaRows, warnings, 2);
        dates = growthWindow.dates();
        resolvedBranches = growthWindow.branches();

        int dataRowCount = 0;
        for (String b : resolvedBranches) {
            Map<String, Double> dmap = delta.getOrDefault(b, Map.of());
            Map<String, Double> bmap = base.getOrDefault(b, Map.of());

            List<Double> y = new ArrayList<>(dates.size());
            for (int i = 0; i < dates.size(); i++) {
                if (i == 0) {
                    y.add(null);
                    continue;
                }
                String d = dates.get(i);
                String prev = dates.get(i - 1);
                Double dv = dmap.get(d);
                Double bv = bmap.get(prev);
                if (dv == null || bv == null || Math.abs(bv) < EPSILON) {
                    y.add(null);
                    continue;
                }
                Double point = dv / bv * 100.0;
                y.add(point);
                dataRowCount++;
            }
            series.add(new SeriesResponse.Series(b, y));
        }

        MetricDef d1 = metricService.metricMap().get(deltaMetric);
        MetricDef d2 = metricService.metricMap().get(baseMetric);
        String t1 = d1 == null ? deltaMetric : d1.name();
        String t2 = d2 == null ? baseMetric : d2.name();

        return new SeriesResponse(
                "增长率: " + resolvedScope + "（" + t1 + " / 上期" + t2 + "）",
                "%",
                dates,
                series,
                warnings,
                new SeriesResponse.Meta(
                        requested,
                        resolvedBranches.size(),
                        requested != resolvedBranches.size(),
                        Math.max(0, requested - resolvedBranches.size()),
                        start + " ~ " + end,
                        resolvedScope,
                        deltaMetric + "/" + baseMetric,
                        dataRowCount,
                        dataRowCount == 0
                )
        );
    }

    private List<String> normalizeAndResolveBranches(String scope, List<String> branches, List<String> warnings) {
        if (branches == null || branches.isEmpty()) {
            return List.of();
        }
        List<String> dictionary = branches(scope);
        Map<String, String> normMap = new LinkedHashMap<>();
        for (String canonical : dictionary) {
            normMap.put(BranchNormalizeUtil.normalizeBranch(canonical), canonical);
        }

        List<String> resolved = new ArrayList<>();
        for (String branch : branches) {
            String normalized = BranchNormalizer.normalizeBranch(branch);
            if (normalized.isBlank()) {
                continue;
            }
            String canonical = normMap.getOrDefault(BranchNormalizeUtil.normalizeBranch(normalized), normalized);
            resolved.add(canonical);
        }

        List<String> deduped = resolved.stream().distinct().toList();
        if (deduped.size() < resolved.size()) {
            warnings.add("网点名称存在空格/换行差异，系统已自动规范并去重。");
        }
        return deduped;
    }

    private DegradedWindow degradeRowsIfNeeded(List<String> dates,
                                               List<String> branches,
                                               List<FactRow> rows,
                                               List<String> warnings,
                                               int seriesCount) {
        if (maxPoints <= 0 || dates.isEmpty() || branches.isEmpty()) {
            return new DegradedWindow(dates, branches, rows);
        }
        long estimatedPoints = (long) branches.size() * dates.size() * Math.max(seriesCount, 1);
        if (estimatedPoints <= maxPoints) {
            return new DegradedWindow(dates, branches, rows);
        }

        int dateStep = (int) Math.ceil((double) estimatedPoints / maxPoints);
        if (dateStep > 1) {
            Set<String> keptDates = new LinkedHashSet<>();
            for (int i = 0; i < dates.size(); i += dateStep) {
                keptDates.add(dates.get(i));
            }
            keptDates.add(dates.get(dates.size() - 1));

            List<String> sampledDates = dates.stream().filter(keptDates::contains).toList();
            List<FactRow> sampledRows = rows.stream().filter(r -> keptDates.contains(r.bizDate())).toList();
            long afterPoints = (long) branches.size() * sampledDates.size() * Math.max(seriesCount, 1);
            warnings.add("数据点过大，已按日期采样(step=" + dateStep + ") 以保障性能。估算点数 " + estimatedPoints + " -> " + afterPoints + "。");
            return new DegradedWindow(sampledDates, branches, sampledRows);
        }
        return new DegradedWindow(dates, branches, rows);
    }

    private String resolveScope(String requestedScope) {
        List<String> availableScopes = factRepo.findScopes();
        if (availableScopes.isEmpty()) {
            return requestedScope == null ? "" : requestedScope.trim();
        }

        String raw = requestedScope == null ? "" : requestedScope.trim();
        if (!raw.isBlank()) {
            for (String available : availableScopes) {
                if (available.equals(raw)) {
                    return available;
                }
            }
            String normalizedInput = DateUtil.normalizeScope(raw);
            for (String available : availableScopes) {
                if (normalizedScopeKey(available).equalsIgnoreCase(normalizedInput)) {
                    return available;
                }
            }
        }
        return availableScopes.get(0);
    }

    private String normalizedScopeKey(String scope) {
        if (scope == null || scope.isBlank()) {
            return "";
        }
        String upper = scope.trim().toUpperCase(Locale.ROOT);
        if ("PHY".equals(upper) || "ADJ".equals(upper)) {
            return upper;
        }
        return DateUtil.normalizeScope(scope);
    }

    public String scopeDisplayName(String scope) {
        if (scope == null || scope.isBlank()) {
            return "";
        }
        String upper = scope.trim().toUpperCase(Locale.ROOT);
        if ("PHY".equals(upper) || "ADJ".equals(upper)) {
            return DateUtil.scopeDisplayName(upper);
        }
        return scope;
    }

    private record DegradedWindow(List<String> dates, List<String> branches, List<FactRow> rows) {
    }

    public Map<String, Object> exportReport(String scope, String date, List<String> metrics) {
        HeatmapResponse hm = heatmap(scope, date, metrics);
        return Map.of(
                "title", "贷款多视角日报分析",
                "date", date,
                "scope", hm.scope(),
                "metrics", hm.metrics(),
                "branches", hm.branches(),
                "heatmapData", hm.data(),
                "min", hm.min(),
                "max", hm.max(),
                "meta", hm.meta()
        );
    }
}
