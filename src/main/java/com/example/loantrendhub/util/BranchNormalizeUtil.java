package com.example.loantrendhub.util;

public final class BranchNormalizeUtil {
    private BranchNormalizeUtil() {
    }

    public record BranchNormalized(String displayCandidate, String normKey) {
    }

    public static BranchNormalized normalize(String raw) {
        if (raw == null) {
            return new BranchNormalized("", "");
        }

        String display = raw
                .replace('\u3000', ' ')
                .replace('\u00A0', ' ')
                .replace("\uFEFF", "")
                .replace("\r", " ")
                .replace("\n", " ")
                .replace("\t", " ")
                .replace("\u200B", "")
                .replace("\u200C", "")
                .replace("\u200D", "")
                .replace("\u2060", "")
                .replace('\u2014', '-')
                .replace('\u2013', '-')
                .replaceAll("\\s+", " ")
                .trim();

        if (display.isEmpty()) {
            return new BranchNormalized("", "");
        }

        String normKey = display
                .replaceAll("[\\s\\-()\\uFF08\\uFF09]", "")
                .trim();

        return new BranchNormalized(display, normKey);
    }

    public static String normalizeBranch(String raw) {
        return normalize(raw).normKey();
    }

    public static String normalizeDisplay(String raw) {
        return normalize(raw).displayCandidate();
    }
}
