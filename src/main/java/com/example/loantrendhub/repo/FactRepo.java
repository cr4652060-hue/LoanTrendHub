package com.example.loantrendhub.repo;

import com.example.loantrendhub.model.FactRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Repository
public class FactRepo {
    private static final String NORMALIZED_SCOPE_SQL = "CASE " +
            "WHEN scope IN ('PHY','实体贷款','实体贷款（纯账面）','实体贷款(纯账面)','纯账面') THEN 'PHY' " +
            "WHEN scope IN ('ADJ','实体贷款（还原剔转）','实体贷款(还原剔转)','还原剔转','还原') THEN 'ADJ' " +
            "ELSE scope END";

    private final JdbcTemplate jdbcTemplate;

    public FactRepo(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public int[] upsertBatch(List<FactRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return new int[0];
        }
        String sql = """
                INSERT INTO fact_metric_daily (biz_date, scope, branch, metric, val, source_file, raw_branch, norm_branch_key)
                VALUES (?,?,?,?,?,?,?,?)
                ON DUPLICATE KEY UPDATE
                  val = VALUES(val),
                  source_file = VALUES(source_file),
                  raw_branch = VALUES(raw_branch),
                  norm_branch_key = VALUES(norm_branch_key)
                """;
        List<Object[]> batchArgs = rows.stream()
                .map(row -> new Object[]{
                        row.bizDate(),
                        row.scope(),
                        row.branch(),
                        row.metric(),
                        row.val(),
                        row.sourceFile(),
                        row.rawBranch(),
                        row.normBranchKey()
                })
                .toList();

        return jdbcTemplate.batchUpdate(sql, batchArgs);
    }

    public void logUnknownBranch(String rawBranch, String normKey, String sourceFile, Integer rowNo) {
        jdbcTemplate.update(
                "INSERT INTO unknown_branch_log(raw_branch, norm_key, source_file, row_no) VALUES (?,?,?,?)",
                rawBranch, normKey, sourceFile, rowNo
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
        String sql = "SELECT DISTINCT " + NORMALIZED_SCOPE_SQL + " AS normalized_scope FROM fact_metric_daily ORDER BY normalized_scope";
        return jdbcTemplate.queryForList(sql, String.class);
    }

    public Map<String, Object> branchDiagnostics(String scope) {
        Map<String, Object> result = new LinkedHashMap<>();
        String normalized = (scope == null || scope.isBlank()) ? "PHY" : scope;

        Integer dictBranches = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM branch_def WHERE enabled = 1",
                Integer.class
        );
        Integer rowsByScope = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM fact_metric_daily WHERE " + NORMALIZED_SCOPE_SQL + " = ?",
                Integer.class,
                normalized
        );
        Integer distinctFactBranches = jdbcTemplate.queryForObject(
                "SELECT COUNT(DISTINCT branch) FROM fact_metric_daily WHERE " + NORMALIZED_SCOPE_SQL + " = ?",
                Integer.class,
                normalized
        );

        result.put("scope", normalized);
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
        String sql = "SELECT DISTINCT biz_date FROM fact_metric_daily WHERE " + NORMALIZED_SCOPE_SQL + " = ? " +
                "AND biz_date BETWEEN ? AND ? ORDER BY biz_date";
        return jdbcTemplate.queryForList(sql, String.class, scope, start, end);
    }

    public List<FactRow> findByDateScopeMetrics(String date, String scope, List<String> metrics) {
        if (metrics == null || metrics.isEmpty()) return List.of();
        String placeholders = String.join(",", java.util.Collections.nCopies(metrics.size(), "?"));
        List<Object> args = new ArrayList<>();
        args.add(date);
        args.add(scope);
        args.addAll(metrics);
        String sql = "SELECT biz_date, " + NORMALIZED_SCOPE_SQL + " AS scope, branch, metric, val, source_file, raw_branch, norm_branch_key FROM fact_metric_daily " +
                "WHERE biz_date=? AND " + NORMALIZED_SCOPE_SQL + " = ? AND metric IN (" + placeholders + ")";
        return jdbcTemplate.query(sql, this::mapFactRow, args.toArray());
    }

    public List<FactRow> findSeriesByMetric(String scope, String metric, List<String> branches, String start, String end) {
        if (branches == null || branches.isEmpty()) return List.of();
        String placeholders = String.join(",", java.util.Collections.nCopies(branches.size(), "?"));
        List<Object> args = new ArrayList<>();
        args.add(scope);
        args.add(metric);
        args.add(start);
        args.add(end);
        args.addAll(branches);
        String sql = "SELECT biz_date, " + NORMALIZED_SCOPE_SQL + " AS scope, branch, metric, val, source_file, raw_branch, norm_branch_key FROM fact_metric_daily " +
                "WHERE " + NORMALIZED_SCOPE_SQL + " = ? AND metric=? AND biz_date BETWEEN ? AND ? AND branch IN (" + placeholders + ") " +
                "ORDER BY biz_date, branch";
        return jdbcTemplate.query(sql, this::mapFactRow, args.toArray());
    }

    public List<FactRow> findSeriesByBranch(String scope, String branch, List<String> metrics, String start, String end) {
        if (metrics == null || metrics.isEmpty()) return List.of();
        String placeholders = String.join(",", java.util.Collections.nCopies(metrics.size(), "?"));
        List<Object> args = new ArrayList<>();
        args.add(scope);
        args.add(branch);
        args.add(start);
        args.add(end);
        args.addAll(metrics);
        String sql = "SELECT biz_date, " + NORMALIZED_SCOPE_SQL + " AS scope, branch, metric, val, source_file, raw_branch, norm_branch_key FROM fact_metric_daily " +
                "WHERE " + NORMALIZED_SCOPE_SQL + " = ? AND branch=? AND biz_date BETWEEN ? AND ? AND metric IN (" + placeholders + ") " +
                "ORDER BY biz_date, metric";
        return jdbcTemplate.query(sql, this::mapFactRow, args.toArray());
    }

    public List<FactRow> findSeries(String scope, String branch, String metric, String start, String end) {
        String sql = "SELECT biz_date, " + NORMALIZED_SCOPE_SQL + " AS scope, branch, metric, val, source_file, raw_branch, norm_branch_key FROM fact_metric_daily " +
                "WHERE " + NORMALIZED_SCOPE_SQL + " = ? AND branch=? AND metric=? AND biz_date BETWEEN ? AND ? ORDER BY biz_date";
        return jdbcTemplate.query(sql, this::mapFactRow, scope, branch, metric, start, end);
    }

    private FactRow mapFactRow(ResultSet rs, int rowNum) throws SQLException {
        return new FactRow(
                rs.getString("biz_date"),
                rs.getString("scope"),
                rs.getString("branch"),
                rs.getString("metric"),
                rs.getObject("val") == null ? null : rs.getDouble("val"),
                rs.getString("source_file"),
                rs.getString("raw_branch"),
                rs.getString("norm_branch_key")
        );
    }
}