package com.example.loantrendhub.util;

public final class BranchNormalizeUtil {
    private BranchNormalizeUtil() {}

    public static String normalizeBranch(String raw) {
        if (raw == null) return "";
        String cleaned = TextCleanUtil.cleanText(raw);
        if (cleaned == null) return "";

        String b = cleaned
                .replace('\u00A0', ' ')
                .replace('\u3000', ' ')
                .replace("\uFEFF", "")
                .replaceAll("[\\u200B-\\u200D\\u2060]", "")
                .trim();
        if (b.isEmpty()) return "";

        b = b.replaceAll("\\s+", " ");
        b = b.replaceAll("[（(][^）)]*[）)]$", "").trim();
        b = b.replaceAll("\\s+", "");
        return b;
    }
}