package com.example.loantrendhub.service;

import com.example.loantrendhub.model.MetricDef;
import com.example.loantrendhub.repo.MetaRepo;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class MetricService {
    private final MetaRepo metaRepo;

    public MetricService(MetaRepo metaRepo) {
        this.metaRepo = metaRepo;
    }

    public List<MetricDef> listMetrics() {
        return metaRepo.findMetrics();
    }

    public Map<String, MetricDef> metricMap() {
        return listMetrics().stream().collect(Collectors.toMap(MetricDef::metric, Function.identity()));
    }
}
