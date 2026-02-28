package com.example.loantrendhub.repo;

import com.example.loantrendhub.model.FactRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Repository
public class FactRepo {
    private final JdbcTemplate jdbcTemplate;

    public FactRepo(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public int[] upsertBatch(List<FactRow> rows) {
        String sql = """
                INSERT INTO fact_trend(biz_date, scope, branch, metric, value, source_file)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(biz_date, scope, branch, metric)
                DO UPDATE SET value = excluded.value, source_file = excluded.source_file
                """;
        return jdbcTemplate.batchUpdate(sql, rows, 500, (ps, row) -> {
            ps.setString(1, row.bizDate());
            ps.setString(2, row.scope());
            ps.setString(3, row.branch());
            ps.setString(4, row.metric());
            ps.setObject(5, row.value());
            ps.setString(6, row.sourceFile());
        });
    }

    public List<String> findScopes() {
        return jdbcTemplate.queryForList("SELECT DISTINCT scope FROM fact_trend ORDER BY scope", String.class);
    }

    public List<String> findBranches(String scope) {
        return jdbcTemplate.queryForList("SELECT DISTINCT branch FROM fact_trend WHERE scope=? ORDER BY branch", String.class, scope);
    }

    public List<String> findDates(String scope, String start, String end) {
        return jdbcTemplate.queryForList(
                "SELECT DISTINCT biz_date FROM fact_trend WHERE scope=? AND biz_date BETWEEN ? AND ? ORDER BY biz_date",
                String.class, scope, start, end);
    }

    public List<FactRow> findByDateScopeMetrics(String date, String scope, List<String> metrics) {
        if (metrics == null || metrics.isEmpty()) return List.of();
        String placeholders = String.join(",", java.util.Collections.nCopies(metrics.size(), "?"));
        List<Object> args = new ArrayList<>();
        args.add(date);
        args.add(scope);
        args.addAll(metrics);
        String sql = "SELECT biz_date, scope, branch, metric, value, source_file FROM fact_trend " +
                "WHERE biz_date=? AND scope=? AND metric IN (" + placeholders + ")";
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
        String sql = "SELECT biz_date, scope, branch, metric, value, source_file FROM fact_trend " +
                "WHERE scope=? AND metric=? AND biz_date BETWEEN ? AND ? AND branch IN (" + placeholders + ") " +
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
        String sql = "SELECT biz_date, scope, branch, metric, value, source_file FROM fact_trend " +
                "WHERE scope=? AND branch=? AND biz_date BETWEEN ? AND ? AND metric IN (" + placeholders + ") " +
                "ORDER BY biz_date, metric";
        return jdbcTemplate.query(sql, this::mapFactRow, args.toArray());
    }

    public List<FactRow> findSeries(String scope, String branch, String metric, String start, String end) {
        return jdbcTemplate.query(
                "SELECT biz_date, scope, branch, metric, value, source_file FROM fact_trend WHERE scope=? AND branch=? AND metric=? AND biz_date BETWEEN ? AND ? ORDER BY biz_date",
                this::mapFactRow, scope, branch, metric, start, end
        );
    }

    private FactRow mapFactRow(ResultSet rs, int rowNum) throws SQLException {
        return new FactRow(
                rs.getString("biz_date"),
                rs.getString("scope"),
                rs.getString("branch"),
                rs.getString("metric"),
                rs.getObject("value") == null ? null : rs.getDouble("value"),
                rs.getString("source_file")
        );
    }
}