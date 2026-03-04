package com.example.loantrendhub.util;

public final class TextCleanUtil {
    private TextCleanUtil() {}

    public static String cleanText(String raw) {
        if (raw == null) return null;
        String text = raw.replace("\uFFFD", "");
        text = text.replaceAll("[\\p{Cntrl}&&[^\\r\\n\\t]]", "");
        text = text.replace('\u00A0', ' ').replace('\u3000', ' ');
        text = text.replaceAll("\\s+", " ").trim();
        return text;
    }
}
