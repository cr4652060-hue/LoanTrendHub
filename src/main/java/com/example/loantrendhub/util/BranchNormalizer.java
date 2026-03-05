package com.example.loantrendhub.util;

public final class BranchNormalizer {
    private BranchNormalizer() {
    }

    public static String normalizeBranch(String raw) {
        return BranchNormalizeUtil.normalizeDisplay(raw);
    }

    public static String normalizeKey(String raw) {
        return BranchNormalizeUtil.normalizeBranch(raw);
    }
}