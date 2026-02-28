package com.example.loantrendhub.repo;

import com.example.loantrendhub.model.MetricDef;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class MetaRepo {
    private final JdbcTemplate jdbcTemplate;

    public MetaRepo(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<MetricDef> findMetrics() {
        return jdbcTemplate.query(
                "SELECT metric, name, unit, kind, base_metric FROM metric_def ORDER BY metric",
                (rs, rowNum) -> new MetricDef(
                        rs.getString("metric"),
                        rs.getString("name"),
                        rs.getString("unit"),
                        rs.getString("kind"),
                        rs.getString("base_metric")
                )
        );
    }
}