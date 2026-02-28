package com.example.loantrendhub.util.ExcelUtil;

public final class HeaderResolver {
    private HeaderResolver() {}

    public static String resolveMetric(String headerText) {
        if (headerText == null) return null;
        String h = headerText.replace(" ", "");

        if (h.contains("较上日") && h.contains("户")) return "DOD_CNT";
        if (h.contains("较上日") && h.contains("余额")) return "DOD_BAL";
        if (h.contains("较上月") && h.contains("户")) return "MOM_CNT";
        if (h.contains("较上月") && h.contains("余额")) return "MOM_BAL";
        if (h.contains("较年初") && h.contains("户")) return "BOY_CNT";
        if (h.contains("较年初") && h.contains("余额")) return "BOY_BAL";
        if (h.contains("增量较同期") && h.contains("户")) return "Y2M_CNT";
        if (h.contains("增量较同期") && h.contains("余额")) return "Y2M_BAL";
        if (h.contains("增幅") && h.contains("户")) return "GR_CNT";
        if (h.contains("增幅") && h.contains("余额")) return "GR_BAL";

        if (h.contains("户数")) return "CNT_TOTAL";
        if (h.contains("余额")) return "BAL_TOTAL";

        return null;
    }
}