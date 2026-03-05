package com.example.loantrendhub.util;

public final class ScopeUtil {
    private ScopeUtil() {
    }

    public static String normalize(String raw) {
        return DateUtil.normalizeScope(raw);
    }

    public static String displayName(String scopeKey) {
        return DateUtil.scopeDisplayName(scopeKey);
    }
}
