package com.example.loantrendhub.repo;

import com.example.loantrendhub.model.FactRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Repository
public class FactRepo {
    private final JdbcTemplate jdbcTemplate;

    public FactRepo(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public int[] upsertBatch(List<FactRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return new int[0];
        }
        String sql = """
                INSERT INTO fact_metric_daily (biz_date, scope, branch, metric, val, source_file)
                VALUES (?,?,?,?,?,?)
                ON DUPLICATE KEY UPDATE
                  val = VALUES(val),
                  source_file = VALUES(source_file)
                """;
        List<Object[]> batchArgs = rows.stream()
                .map(row -> new Object[]{
                        row.bizDate(),
                        row.scope(),
                        row.branch(),
                        row.metric(),
                        row.val(),
                        row.sourceFile()
                })
                .toList();

        return jdbcTemplate.batchUpdate(sql, batchArgs);
    }

    public void logImportReject(String sourceFile, Integer rowNo, String rawBranch, String normKey, String reason) {
        jdbcTemplate.update(
                "INSERT INTO import_reject(source_file, row_no, raw_branch, norm_key, reason) VALUES (?,?,?,?,?)",
                sourceFile,
                rowNo,
                rawBranch,
                normKey,
                reason
        );
    }

    public Map<String, String> dateRange() {
        String sql = "SELECT MIN(biz_date) AS min_date, MAX(biz_date) AS max_date FROM fact_metric_daily";
        return jdbcTemplate.query(sql, rs -> {
            String min = "";
            String max = "";
            if (rs.next()) {
                String a = rs.getString("min_date");
                String b = rs.getString("max_date");
                min = a == null ? "" : a;
                max = b == null ? "" : b;
            }
            return Map.of("min", min, "max", max);
        });
    }

    public List<String> findScopes() {
        return jdbcTemplate.queryForList(
                "SELECT DISTINCT scope FROM fact_metric_daily WHERE scope IS NOT NULL AND TRIM(scope) <> '' ORDER BY scope",
                String.class
        );
    }

    public List<ScopeStat> findScopeStats(String targetDate) {
        String effectiveDate = (targetDate == null || targetDate.isBlank()) ? "__NO_DATE__" : targetDate.trim();
        return jdbcTemplate.query(
                """
                SELECT scope,
                       MIN(biz_date) AS min_date,
                       MAX(biz_date) AS max_date,
                       COUNT(1) AS total_rows,
                       SUM(CASE WHEN biz_date = ? THEN 1 ELSE 0 END) AS rows_on_target
                FROM fact_metric_daily
                WHERE scope IS NOT NULL AND TRIM(scope) <> ''
                GROUP BY scope
                ORDER BY scope
                """,
                (rs, rowNum) -> new ScopeStat(
                        rs.getString("scope"),
                        rs.getString("min_date"),
                        rs.getString("max_date"),
                        rs.getLong("total_rows"),
                        rs.getLong("rows_on_target")
                ),
                effectiveDate
        );
    }

    public Map<String, Object> branchDiagnostics(String scope) {
        Map<String, Object> result = new LinkedHashMap<>();
        String effectiveScope = (scope == null || scope.isBlank()) ? "" : scope.trim();

        Integer dictBranches = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM branch_def WHERE enabled = 1",
                Integer.class
        );
        Integer rowsByScope;
        Integer distinctFactBranches;
        if (effectiveScope.isBlank()) {
            rowsByScope = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM fact_metric_daily", Integer.class);
            distinctFactBranches = jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT branch) FROM fact_metric_daily", Integer.class);
        } else {
            rowsByScope = jdbcTemplate.queryForObject(
                    "SELECT COUNT(1) FROM fact_metric_daily WHERE scope = ?",
                    Integer.class,
                    effectiveScope
            );
            distinctFactBranches = jdbcTemplate.queryForObject(
                    "SELECT COUNT(DISTINCT branch) FROM fact_metric_daily WHERE scope = ?",
                    Integer.class,
                    effectiveScope
            );
        }

        result.put("scope", effectiveScope);
        result.put("dictBranches", dictBranches == null ? 0 : dictBranches);
        result.put("rowsByScope", rowsByScope == null ? 0 : rowsByScope);
        result.put("distinctFactBranches", distinctFactBranches == null ? 0 : distinctFactBranches);
        return result;
    }

    public List<String> findBranches(String scope) {
        return jdbcTemplate.queryForList(
                "SELECT branch FROM branch_def WHERE enabled = 1 ORDER BY sort_no, branch",
                String.class
        );
    }

    public List<String> findAllBranches() {
        return findBranches(null);
    }

    public Map<String, String> findBranchAliasRawMap() {
        return jdbcTemplate.query(
                "SELECT raw_branch, canon_branch FROM branch_alias WHERE enabled = 1",
                rs -> {
                    Map<String, String> map = new LinkedHashMap<>();
                    while (rs.next()) {
                        map.put(rs.getString("raw_branch"), rs.getString("canon_branch"));
                    }
                    return map;
                }
        );
    }

    public Map<String, String> findBranchAliasNormMap() {
        return jdbcTemplate.query(
                "SELECT norm_key, canon_branch FROM branch_alias WHERE enabled = 1",
                rs -> {
                    Map<String, String> map = new LinkedHashMap<>();
                    while (rs.next()) {
                        map.put(rs.getString("norm_key"), rs.getString("canon_branch"));
                    }
                    return map;
                }
        );
    }

    public int countEnabledBranches() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM branch_def WHERE enabled = 1", Integer.class);
        return count == null ? 0 : count;
    }

    public int countMetricDefs() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM metric_def", Integer.class);
        return count == null ? 0 : count;
    }

    public List<String> findDates(String scope, String start, String end) {
        if (scope == null || scope.isBlank()) {
            String sql = "SELECT DISTINCT biz_date FROM fact_metric_daily WHERE biz_date BETWEEN ? AND ? ORDER BY biz_date";
            return jdbcTemplate.queryForList(sql, String.class, start, end);
        }
        String sql = "SELECT DISTINCT biz_date FROM fact_metric_daily WHERE scope = ? AND biz_date BETWEEN ? AND ? ORDER BY biz_date";
        return jdbcTemplate.queryForList(sql, String.class, scope, start, end);
    }

    public List<FactRow> findByDateScopeMetrics(String date, String scope, List<String> metrics) {
        if (scope == null || scope.isBlank() || metrics == null || metrics.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", Collections.nCopies(metrics.size(), "?"));
        List<Object> args = new ArrayList<>();
        args.add(date);
        args.add(scope);
        args.addAll(metrics);
        String sql = "SELECT biz_date, scope, branch, metric, val, source_file FROM fact_metric_daily " +
                "WHERE biz_date = ? AND scope = ? AND metric IN (" + placeholders + ")";
        return jdbcTemplate.query(sql, this::mapFactRow, args.toArray());
    }

    public List<FactRow> findSeriesByMetric(String scope, String metric, List<String> branches, String start, String end) {
        if (scope == null || scope.isBlank() || branches == null || branches.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", Collections.nCopies(branches.size(), "?"));
        List<Object> args = new ArrayList<>();
        args.add(scope);
        args.add(metric);
        args.add(start);
        args.add(end);
        args.addAll(branches);
        String sql = "SELECT biz_date, scope, branch, metric, val, source_file FROM fact_metric_daily " +
                "WHERE scope = ? AND metric = ? AND biz_date BETWEEN ? AND ? AND branch IN (" + placeholders + ") " +
                "ORDER BY biz_date, branch";
        return jdbcTemplate.query(sql, this::mapFactRow, args.toArray());
    }

    public List<FactRow> findSeriesByBranch(String scope, String branch, List<String> metrics, String start, String end) {
        if (scope == null || scope.isBlank() || metrics == null || metrics.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", Collections.nCopies(metrics.size(), "?"));
        List<Object> args = new ArrayList<>();
        args.add(scope);
        args.add(branch);
        args.add(start);
        args.add(end);
        args.addAll(metrics);
        String sql = "SELECT biz_date, scope, branch, metric, val, source_file FROM fact_metric_daily " +
                "WHERE scope = ? AND branch = ? AND biz_date BETWEEN ? AND ? AND metric IN (" + placeholders + ") " +
                "ORDER BY biz_date, metric";
        return jdbcTemplate.query(sql, this::mapFactRow, args.toArray());
    }

    public List<FactRow> findSeries(String scope, String branch, String metric, String start, String end) {
        if (scope == null || scope.isBlank()) {
            return List.of();
        }
        String sql = "SELECT biz_date, scope, branch, metric, val, source_file FROM fact_metric_daily " +
                "WHERE scope = ? AND branch = ? AND metric = ? AND biz_date BETWEEN ? AND ? ORDER BY biz_date";
        return jdbcTemplate.query(sql, this::mapFactRow, scope, branch, metric, start, end);
    }

    public List<HeatmapCell> findHeatmapMatrix(String date, String scope, List<String> metrics) {
        if (date == null || date.isBlank() || scope == null || scope.isBlank() || metrics == null || metrics.isEmpty()) {
            return List.of();
        }
        String metricTable = metrics.stream()
                .map(v -> "SELECT ? AS metric")
                .collect(Collectors.joining(" UNION ALL "));
        String sql = """
                SELECT b.branch,
                       m.metric,
                       f.val AS val
                FROM branch_def b
                CROSS JOIN (
                """ + metricTable + """
                ) m
                LEFT JOIN fact_metric_daily f
                  ON f.branch = b.branch
                 AND f.biz_date = ?
                 AND f.scope = ?
                 AND f.metric = m.metric
                WHERE b.enabled = 1
                ORDER BY b.sort_no, b.branch
                """;
        List<Object> args = new ArrayList<>(metrics.size() + 2);
        args.addAll(metrics);
        args.add(date);
        args.add(scope);
        return jdbcTemplate.query(sql, (rs, rowNum) -> new HeatmapCell(
                rs.getString("branch"),
                rs.getString("metric"),
                rs.getObject("val") == null ? null : rs.getDouble("val")
        ), args.toArray());
    }

    public boolean metricExistsInScope(String scope, String metric) {
        if (scope == null || scope.isBlank() || metric == null || metric.isBlank()) {
            return false;
        }
        Integer one = jdbcTemplate.query(
                "SELECT 1 FROM fact_metric_daily WHERE scope = ? AND metric = ? LIMIT 1",
                rs -> rs.next() ? 1 : 0,
                scope,
                metric
        );
        return one != null && one == 1;
    }

    public Map<String, Long> countRowsBySourceFileByBizDate(String sourceFile) {
        return countRowsBySourceFile(sourceFile, "biz_date");
    }

    public Map<String, Long> countRowsBySourceFileByScope(String sourceFile) {
        return countRowsBySourceFile(sourceFile, "scope");
    }

    public Map<String, Long> countRowsBySourceFileByMetric(String sourceFile) {
        return countRowsBySourceFile(sourceFile, "metric");
    }

    private Map<String, Long> countRowsBySourceFile(String sourceFile, String groupColumn) {
        if (sourceFile == null || sourceFile.isBlank()) {
            return Map.of();
        }
        String safeColumn = switch (groupColumn) {
            case "biz_date", "scope", "metric" -> groupColumn;
            default -> throw new IllegalArgumentException("Unsupported group column: " + groupColumn);
        };
        String sql = "SELECT " + safeColumn + " AS group_key, COUNT(1) AS cnt " +
                "FROM fact_metric_daily WHERE source_file = ? " +
                "GROUP BY " + safeColumn + " ORDER BY cnt DESC, group_key";
        return jdbcTemplate.query(sql, rs -> {
            Map<String, Long> map = new LinkedHashMap<>();
            while (rs.next()) {
                map.put(String.valueOf(rs.getString("group_key")), rs.getLong("cnt"));
            }
            return map;
        }, sourceFile);
    }

    private FactRow mapFactRow(ResultSet rs, int rowNum) throws SQLException {
        return new FactRow(
                rs.getString("biz_date"),
                rs.getString("scope"),
                rs.getString("branch"),
                rs.getString("metric"),
                rs.getObject("val") == null ? null : rs.getDouble("val"),
                rs.getString("source_file"),
                null,
                null
        );
    }

    public record ScopeStat(String scope,
                            String minDate,
                            String maxDate,
                            long totalRows,
                            long rowsOnTargetDate) {
    }

    public record HeatmapCell(String branch,
                              String metric,
                              Double val) {
    }
}
