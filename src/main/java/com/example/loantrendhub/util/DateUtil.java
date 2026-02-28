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
}