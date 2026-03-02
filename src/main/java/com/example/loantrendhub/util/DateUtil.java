package com.example.loantrendhub.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DateUtil {
    private static final DateTimeFormatter STD = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final Pattern DATE_PATTERN = Pattern.compile("(20\\d{2})[-._]?(\\d{1,2})[-._]?(\\d{1,2})");

    private DateUtil() {}

    public static String normalizeDate(String raw) {
        if (raw == null || raw.isBlank()) return null;
        String s = raw.trim().replace('/', '-').replace('.', '-').replace('_', '-');
        try {
            return LocalDate.parse(s, STD).format(STD);
        } catch (DateTimeParseException ignored) {
        }
        Matcher m = DATE_PATTERN.matcher(raw);
        if (m.find()) {
            int y = Integer.parseInt(m.group(1));
            int mo = Integer.parseInt(m.group(2));
            int d = Integer.parseInt(m.group(3));
            return LocalDate.of(y, mo, d).format(STD);
        }
        return null;
    }


    public static String normalizeScope(String raw) {
        if (raw == null) return "PHY";
        String text = raw.trim();
        if (text.isEmpty()) return "PHY";
        String compact = text.replace("（", "(").replace("）", ")").replace(" ", "");
        return switch (compact.toUpperCase()) {
            case "PHY", "实体贷款", "实体贷款(纯账面)", "纯账面" -> "PHY";
            case "ADJ", "实体贷款(还原剔转)", "还原剔转", "还原" -> "ADJ";
            default -> "PHY";
        };
    }

    public static String scopeDisplayName(String scopeKey) {
        return switch (normalizeScope(scopeKey)) {
            case "ADJ" -> "实体贷款（还原剔转）";
            default -> "实体贷款（纯账面）";
        };
    }
}