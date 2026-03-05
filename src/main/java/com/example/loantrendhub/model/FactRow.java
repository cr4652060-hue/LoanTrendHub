package com.example.loantrendhub.model;

public record FactRow(
        String bizDate,
        String scope,
        String branch,
        String metric,
        Double val,
        String sourceFile,
        String rawBranch,
        String normBranchKey
) {
    public FactRow(String bizDate,
                   String scope,
                   String branch,
                   String metric,
                   Double val,
                   String sourceFile) {
        this(bizDate, scope, branch, metric, val, sourceFile, null, null);
    }
}