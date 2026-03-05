package com.example.loantrendhub.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DateUtil {
    private static final DateTimeFormatter STD = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final Pattern DATE_PATTERN = Pattern.compile("(20\\d{2})[-._]?(\\d{1,2})[-._]?(\\d{1,2})");

    private DateUtil() {
    }

    public static String normalizeDate(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
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
        if (raw == null) {
            return "PHY";
        }
        String compact = raw.trim()
                .replace('\uFF08', '(')
                .replace('\uFF09', ')')
                .replaceAll("\\s+", "");
        if (compact.isEmpty()) {
            return "PHY";
        }

        String upper = compact.toUpperCase();
        if ("ADJ".equals(upper) || compact.contains("\u8fd8\u539f") || compact.contains("\u5265\u8f6c")) {
            return "ADJ";
        }
        if ("PHY".equals(upper) || compact.contains("\u7eaf\u8d26\u9762") || compact.contains("\u5b9e\u4f53\u8d37\u6b3e")) {
            return "PHY";
        }
        return "PHY";
    }

    public static String scopeDisplayName(String scopeKey) {
        return switch (normalizeScope(scopeKey)) {
            case "ADJ" -> "\u5b9e\u4f53\u8d37\u6b3e\uff08\u8fd8\u539f\u5265\u8f6c\uff09";
            default -> "\u5b9e\u4f53\u8d37\u6b3e\uff08\u7eaf\u8d26\u9762\uff09";
        };
    }
}