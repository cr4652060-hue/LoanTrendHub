
public final class ScopeUtil {
    private ScopeUtil() {}

    public static String normalize(String raw) {
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

    public static String displayName(String scopeKey) {
        return switch (normalize(scopeKey)) {
            case "ADJ" -> "实体贷款（还原剔转）";
            default -> "实体贷款（纯账面）";
        };
    }
}