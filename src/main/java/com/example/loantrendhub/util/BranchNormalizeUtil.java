package com.example.loantrendhub.util;

public final class BranchNormalizeUtil {
    private BranchNormalizeUtil() {}

    public record BranchNormalized(String displayCandidate, String normKey) {}

    public static BranchNormalized normalize(String raw) {
        if (raw == null) {
            return new BranchNormalized("", "");
        }
        String cleaned = TextCleanUtil.cleanText(raw);
        if (cleaned == null) {
            return new BranchNormalized("", "");
        }

        String display = cleaned
                .replace('\u3000', ' ')
                .replace('\u00A0', ' ')
                .replace("\uFEFF", "")
                .replaceAll("[\u200B\u200C\u200D\u2060\t\r\n]", "")
                .replace('（', '(')
                .replace('）', ')')
                .replace('【', '(')
                .replace('】', ')')
                .replace('－', '-')
                .replace('—', '-')
                .replace('–', '-')
                .replace('―', '-')
                .replaceAll("\\s+", " ")
                .trim();

        if (display.isEmpty()) {
            return new BranchNormalized("", "");
        }

        display = display
                .replace("營業部", "营业部")
                .replace("經營中心", "经营中心")
                .replace("分公司", "公司");

        String normKey = display
                .replaceAll("[()\-\\s]", "")
                .trim();

        return new BranchNormalized(display, normKey);
    }

    public static String normalizeBranch(String raw) {
        return normalize(raw).normKey();
    }
}