package com.example.loantrendhub.service;

import com.example.loantrendhub.model.FactRow;
import com.example.loantrendhub.model.HeatmapResponse;
import com.example.loantrendhub.model.MetricDef;
import com.example.loantrendhub.model.SeriesResponse;
import com.example.loantrendhub.repo.FactRepo;
import com.example.loantrendhub.util.BranchNormalizeUtil;
import com.example.loantrendhub.util.BranchNormalizer;
import com.example.loantrendhub.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.Arrays;
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
    private static final Logger log = LoggerFactory.getLogger(QueryService.class);
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
        int aliasCount = factRepo.countBranchAlias();
        int metricCount = factRepo.countMetricDefs();
        boolean factExists = factRepo.factTableExists();
        if (branchCount <= 0 || aliasCount <= 0 || metricCount <= 0 || !factExists) {
            throw new MetadataNotReadyException("数据库初始化未完成，请检查 schema-mysql.sql 是否执行");
        }
    }

    public Map<String, Object> dateRange() {
        return dateRangeByScope(null);
    }

    public Map<String, Object> dateRangeByScope(String scope) {
        ensureMetadataReady();
        String resolvedScope = scope == null || scope.isBlank() ? "" : resolveScope(scope);
        Map<String, String> raw = resolvedScope.isBlank() ? factRepo.dateRange() : factRepo.dateRangeByScope(resolvedScope);
        String min = raw.getOrDefault("min", "");
        String max = raw.getOrDefault("max", "");
        boolean hasData = min != null && !min.isBlank() && max != null && !max.isBlank();
        return Map.of(
                "scope", resolvedScope,
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
    public String latestHeatmapDate(String scope, List<String> metrics) {
        String resolvedScope = resolveScope(scope);
        List<String> selectedMetrics = (metrics == null ? List.<String>of() : metrics).stream()
                .map(v -> v == null ? "" : v.trim())
                .filter(v -> !v.isBlank())
                .distinct()
                .toList();
        if (selectedMetrics.isEmpty()) {
            String max = dateRangeByScope(resolvedScope).getOrDefault("max", "").toString();
            return max.isBlank() ? null : max;
        }
        List<String> branches = branches(resolvedScope);
        if (branches.isEmpty()) {
            return null;
        }
        Map<String, MetricDef> defs = metricService.metricMap();
        List<String> dates = factRepo.findDatesDesc(resolvedScope);
        for (String d : dates) {
            Map<String, Double> matrixValues = buildHeatmapMatrixValues(resolvedScope, d, branches, selectedMetrics, defs);
            boolean anyData = matrixValues.values().stream().anyMatch(v -> v != null && Double.isFinite(v));
            if (anyData) {
                return d;
            }
        }
        return null;
    }

    private Map<String, Double> buildHeatmapMatrixValues(String scope,
                                                         String date,
                                                         List<String> branches,
                                                         List<String> selectedMetrics,
                                                         Map<String, MetricDef> defs) {
        List<String> rawMetrics = new ArrayList<>();
        List<String> derivedMetrics = new ArrayList<>();
        for (String metric : selectedMetrics) {
            MetricDef md = defs.get(metric);
            if (md != null && ("DELTA".equalsIgnoreCase(md.kind()) || "RATE".equalsIgnoreCase(md.kind()))) {
                derivedMetrics.add(metric);
            } else {
                rawMetrics.add(metric);
            }
        }

        Map<String, Double> matrixValues = new HashMap<>();
        if (!rawMetrics.isEmpty()) {
            List<FactRepo.HeatmapCell> source = factRepo.findHeatmapMatrix(date, scope, rawMetrics);
            for (FactRepo.HeatmapCell row : source) {
                matrixValues.put(row.branch() + "\u0001" + row.metric(), row.val());
            }
        }
        if (!derivedMetrics.isEmpty()) {
            matrixValues.putAll(computeDerivedHeatmapValues(scope, date, branches, derivedMetrics, defs));
        }
        return matrixValues;
    }

    private boolean isLevelMetric(String metric, Map<String, MetricDef> defs) {
        MetricDef md = defs.get(metric);
        return md != null && "LEVEL".equalsIgnoreCase(md.kind());
    }

    private Double normalizeHeatColorValue(Double rawValue,
                                           Double minVal,
                                           Double maxVal,
                                           boolean levelMetric) {
        if (rawValue == null || minVal == null || maxVal == null || !Double.isFinite(rawValue) || !Double.isFinite(minVal) || !Double.isFinite(maxVal)) {
            return null;
        }
        if (levelMetric) {
            if (Double.compare(minVal, maxVal) == 0) {
                return 0d;
            }
            double norm01 = (rawValue - minVal) / (maxVal - minVal);
            double scaled = (norm01 * 2.0) - 1.0;
            return Math.max(-1d, Math.min(1d, scaled));
        }

        double scaled;
        if (minVal < 0d && maxVal > 0d) {
            double absMax = Math.max(Math.abs(minVal), Math.abs(maxVal));
            if (absMax < EPSILON) {
                return 0d;
            }
            scaled = rawValue / absMax;
        } else if (minVal >= 0d && maxVal > 0d) {
            if (Double.compare(minVal, maxVal) == 0) {
                scaled = 0.6d;
            } else {
                double norm01 = (rawValue - minVal) / (maxVal - minVal);
                scaled = 0.2d + norm01 * 0.8d;
            }
        } else if (minVal < 0d && maxVal <= 0d) {
            if (Double.compare(minVal, maxVal) == 0) {
                scaled = -0.6d;
            } else {
                double norm01 = (rawValue - minVal) / (maxVal - minVal);
                scaled = -1d + norm01 * 0.8d;
            }
        } else {
            scaled = 0d;
        }
        return Math.max(-1d, Math.min(1d, scaled));
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
                    -1d,
                    1d,
                    List.of(),
                    List.of(),
                    Map.of(),
                    new HeatmapResponse.Meta(date, resolvedScope, "", 0, true)
            );
        }
        String effectiveDate = (date == null || date.isBlank()) ? latestHeatmapDate(resolvedScope, selectedMetrics) : date;
        Map<String, Double> matrixValues = buildHeatmapMatrixValues(resolvedScope, effectiveDate, branches, selectedMetrics, defs);
        boolean hasAnyData = matrixValues.values().stream().anyMatch(v -> v != null && Double.isFinite(v));
        if (!hasAnyData) {
            String latest = latestHeatmapDate(resolvedScope, selectedMetrics);
            if (latest != null && !latest.isBlank() && !latest.equals(effectiveDate)) {
                effectiveDate = latest;
                matrixValues = buildHeatmapMatrixValues(resolvedScope, effectiveDate, branches, selectedMetrics, defs);
            }
        }
        Map<String, Integer> metricIdx = new HashMap<>();
        for (int i = 0; i < selectedMetrics.size(); i++) {
            metricIdx.put(selectedMetrics.get(i), i);
        }

        Map<String, Integer> branchIdx = new HashMap<>();
        for (int i = 0; i < branches.size(); i++) {
            branchIdx.put(branches.get(i), i);
        }

        List<List<Object>> data = new ArrayList<>();
        List<HeatmapResponse.Cell> cells = new ArrayList<>();
        int dataRowCount = 0;
        int debugLogCount = 0;
        Map<String, Double> metricMin = new HashMap<>();
        Map<String, Double> metricMax = new HashMap<>();
        for (String metric : selectedMetrics) {
            Double minVal = null;
            Double maxVal = null;
            for (String branch : branches) {
                Double v = matrixValues.get(branch + "\u0001" + metric);
                if (v == null || !Double.isFinite(v)) {
                    continue;
                }
                minVal = minVal == null ? v : Math.min(minVal, v);
                maxVal = maxVal == null ? v : Math.max(maxVal, v);
            }
            metricMin.put(metric, minVal);
            metricMax.put(metric, maxVal);
        }
        for (String branch : branches) {
            Integer yi = branchIdx.get(branch);
            if (yi == null) {
                continue;
            }
            for (String metric : selectedMetrics) {
                Integer xi = metricIdx.get(metric);
                if (xi == null) {
                    continue;
                }
                Double rawValue = matrixValues.get(branch + "\u0001" + metric);
                boolean hasData = rawValue != null && Double.isFinite(rawValue);
                Double safeRaw = hasData ? rawValue : null;
                Double colorValue = hasData
                        ? normalizeHeatColorValue(safeRaw, metricMin.get(metric), metricMax.get(metric), isLevelMetric(metric, defs))
                        : null;
                data.add(Arrays.asList(xi, yi, colorValue, safeRaw, hasData));
                cells.add(new HeatmapResponse.Cell(branch, metric, safeRaw, hasData));
                if (hasData && debugLogCount < 8 && log.isDebugEnabled()) {
                    log.debug("heat cell metric={}, branch={}, raw={}, color={}", metric, branch, safeRaw, colorValue);
                    debugLogCount++;
                }
                if (hasData) {
                    dataRowCount++;
                }
            }
        }

        boolean allNull = dataRowCount == 0;

        Map<String, HeatmapResponse.MetricMeta> metricDefs = new LinkedHashMap<>();
        for (String metric : selectedMetrics) {
            MetricDef d = defs.get(metric);
            metricDefs.put(metric, d == null ? new HeatmapResponse.MetricMeta(metric, "") : new HeatmapResponse.MetricMeta(d.name(), d.unit()));
        }

        return new HeatmapResponse(
                effectiveDate,
                resolvedScope,
                selectedMetrics,
                branches,
                -1d,
                1d,
                data,
                cells,
                metricDefs,
                new HeatmapResponse.Meta(
                        effectiveDate,
                        resolvedScope,
                        String.join(",", selectedMetrics),
                        dataRowCount,
                        allNull
                )
        );
    }


    private Map<String, Double> computeDerivedHeatmapValues(String scope,
                                                            String date,
                                                            List<String> branches,
                                                            List<String> metrics,
                                                            Map<String, MetricDef> defs) {
        if (branches == null || branches.isEmpty() || metrics == null || metrics.isEmpty()) {
            return Map.of();
        }
        List<String> dates = new ArrayList<>(new TreeSet<>(factRepo.findDates(scope, "0001-01-01", date)));
        if (dates.isEmpty()) {
            return Map.of();
        }
        String start = dates.get(0);
        Map<String, Double> values = new HashMap<>();
        for (String metric : metrics) {
            MetricDef md = defs.get(metric);
            if (md == null || md.baseMetric() == null || md.baseMetric().isBlank()) {
                continue;
            }
            List<FactRow> rows;
            if ("DELTA".equalsIgnoreCase(md.kind())) {
                rows = buildDeltaRowsFromBase(scope, branches, dates, metric, md.baseMetric(), start, date);
            } else if ("RATE".equalsIgnoreCase(md.kind())) {
                rows = buildRateRowsFromBase(scope, branches, dates, metric, md.baseMetric(), start, date);
            } else {
                continue;
            }
            for (FactRow row : rows) {
                if (!date.equals(row.bizDate()) || row.val() == null || !Double.isFinite(row.val())) {
                    continue;
                }
                values.put(row.branch() + "\u0001" + metric, row.val());
            }
        }
        return values;
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
