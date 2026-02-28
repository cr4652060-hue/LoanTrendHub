package com.example.loantrendhub.service;

import com.example.loantrendhub.model.FactRow;
import com.example.loantrendhub.repo.FactRepo;
import com.example.loantrendhub.util.ScopeUtil;
import com.example.loantrendhub.util.DateUtil;
import com.example.loantrendhub.util.ExcelUtil.HeaderResolver;
import org.apache.poi.ss.usermodel.*;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class IngestService {
    private static final Pattern DATE_PATTERN = Pattern.compile("(20\\d{2})[.-/](\\d{1,2})[.-/](\\d{1,2})");
    private static final Set<String> INVALID_BRANCH = Set.of("单位", "合计", "各项贷款", "总计");
    private final FactRepo factRepo;

    public IngestService(FactRepo factRepo) {
        this.factRepo = factRepo;
    }

    public Map<String, Object> ingest(List<MultipartFile> files) throws Exception {
        List<FactRow> all = new ArrayList<>();
        List<String> accepted = new ArrayList<>();
        List<String> messages = new ArrayList<>();

        for (MultipartFile file : files) {
            if (file == null || file.isEmpty()) continue;

            String source = file.getOriginalFilename() == null ? "unknown.xlsx" : file.getOriginalFilename();
            String bizDate = extractBizDate(source);

            try (InputStream in = file.getInputStream(); Workbook wb = WorkbookFactory.create(in)) {
                Sheet sheet = wb.getSheetAt(0);
                if (bizDate == null) bizDate = extractBizDateFromSheet(sheet);
                if (bizDate == null) {
                    messages.add("[SKIP] " + source + "：未识别到业务日期");
                    continue;
                }
                List<FactRow> parsed = parseSheet(sheet, bizDate, source);
                all.addAll(parsed);
                accepted.add(source);
                messages.add("[OK] " + source + "：date=" + bizDate + " rows=" + parsed.size());
            } catch (Exception ex) {
                messages.add("[ERR] " + source + "：" + ex.getMessage());
                throw ex;
            }
        }

        int saved = all.isEmpty() ? 0 : Arrays.stream(factRepo.upsertBatch(all)).sum();
        return Map.of("acceptedFiles", accepted, "rows", all.size(), "saved", saved, "messages", messages);
    }

    private List<FactRow> parseSheet(Sheet sheet, String bizDate, String sourceFile) {
        DataFormatter formatter = new DataFormatter();

        int maxCol = 0;
        for (Row row : sheet) if (row != null && row.getLastCellNum() > maxCol) maxCol = row.getLastCellNum();

        // ✅ 识别表头深度：前 12 行里，最后一个“像表头”的行作为 headerDepth
        int headerDepth = detectHeaderDepth(sheet, formatter);

        // ✅ 识别“单位/网点”所在列：优先找“单位”，找不到再用采样推断
        int branchCol = detectBranchCol(sheet, formatter, headerDepth);

        Map<Integer, String> scopeByCol = detectScopeByColumn(sheet, maxCol, formatter);

        // metricByCol：把每列表头拼起来 -> metric
        Map<Integer, String> metricByCol = new HashMap<>();
        for (int c = 0; c < maxCol; c++) {
            StringBuilder header = new StringBuilder();
            for (int r = 0; r <= headerDepth; r++) {
                Row row = sheet.getRow(r);
                if (row == null) continue;
                String txt = formatter.formatCellValue(row.getCell(c));
                if (!txt.isBlank()) header.append(' ').append(txt.trim());
            }
            String metric = HeaderResolver.resolveMetric(header.toString());
            if (metric != null) metricByCol.put(c, metric);
        }

        int dataStart = headerDepth + 1;
        List<FactRow> rows = new ArrayList<>();

        for (int r = dataStart; r <= sheet.getLastRowNum(); r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;

            String branch = formatter.formatCellValue(row.getCell(branchCol)).trim();
            if (branch.isBlank()) continue;
            if (branch.contains("各项贷款") || branch.contains("单位") || branch.contains("制表") || branch.contains("注")) continue;

            for (Map.Entry<Integer, String> e : metricByCol.entrySet()) {
                int c = e.getKey();
                String scope = ScopeUtil.normalize(scopeByCol.getOrDefault(c, "PHY"));
                Double value = parseNumeric(row.getCell(c), formatter);
                if (value == null) continue;
                rows.add(new FactRow(bizDate, scope, branch, e.getValue(), value, sourceFile));
            }
        }

        if (rows.isEmpty()) {
            throw new IllegalArgumentException("解析结果为空：请确认表头未被改坏，且“单位/网点”列可识别（常见在B列）");
        }
        return rows;
    }

    private int detectHeaderDepth(Sheet sheet, DataFormatter formatter) {
        int max = Math.min(12, sheet.getLastRowNum());
        int depth = 5; // 默认：0..5 作为表头（你这类日报表基本够）
        for (int r = 0; r <= max; r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;
            String line = rowToText(row, formatter).replace(" ", "");
            // 命中明显表头关键词则更新 depth
            if (line.contains("实体贷款") || line.contains("纯账面") || line.contains("还原") ||
                    line.contains("较上日") || line.contains("较上月") || line.contains("较年初") ||
                    line.contains("增量较同期") || line.contains("增幅") ||
                    line.contains("户数") || line.contains("余额") || line.contains("单位")) {
                depth = r;
            }
        }
        return depth;
    }

    private int detectBranchCol(Sheet sheet, DataFormatter formatter, int headerDepth) {
        // 1) 优先在表头区找 “单位”
        int maxCol = 0;
        for (int r = 0; r <= headerDepth; r++) {
            Row row = sheet.getRow(r);
            if (row != null && row.getLastCellNum() > maxCol) maxCol = row.getLastCellNum();
        }
        for (int r = 0; r <= headerDepth; r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;
            for (int c = 0; c < Math.max(maxCol, row.getLastCellNum()); c++) {
                String t = formatter.formatCellValue(row.getCell(c)).trim();
                if ("单位".equals(t) || "网点".equals(t) || t.contains("单位")) return c;
            }
        }
        // 2) 采样 dataStart..dataStart+10：哪个列更像“网点名”就用哪个
        int dataStart = headerDepth + 1;
        int sampleEnd = Math.min(sheet.getLastRowNum(), dataStart + 10);

        int[] scores = new int[Math.max(maxCol, 2)];
        for (int r = dataStart; r <= sampleEnd; r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;

            for (int c = 0; c < scores.length; c++) {
                String text = formatter.formatCellValue(row.getCell(c)).trim();
                if (!text.isBlank() && looksLikeBranch(text)) scores[c]++;
            }
        }

        int bestCol = 0;
        int bestScore = -1;
        for (int c = 0; c < scores.length; c++) {
            if (scores[c] > bestScore) {
                bestScore = scores[c];
                bestCol = c;
            }
        }
        return bestCol;
    }

    private boolean looksLikeBranch(String s) {
        // 你的网点名一般包含：支行/分理处/营业部/信用社 等
        return s.contains("支行") || s.contains("分理处") || s.contains("营业部") || s.contains("信用社") || s.length() >= 2;
    }

    private String rowToText(Row row, DataFormatter formatter) {
        StringBuilder sb = new StringBuilder();
        short last = row.getLastCellNum();
        for (int c = 0; c < last; c++) {
            String t = formatter.formatCellValue(row.getCell(c));
            if (!t.isBlank()) sb.append(t).append(' ');
        }
        return sb.toString();
    }

    private Map<Integer, String> detectScopeByColumn(Sheet sheet, int maxCol, DataFormatter formatter) {
        Map<Integer, String> res = new HashMap<>();
        int adjStart = maxCol / 2;

        for (int r = 0; r <= Math.min(8, sheet.getLastRowNum()); r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;
            for (int c = 0; c < maxCol; c++) {
                String t = formatter.formatCellValue(row.getCell(c)).replace(" ", "");
                if (t.contains("还原") || t.contains("剔转") || t.contains("核销")) {
                    adjStart = Math.min(adjStart, c);
                }
            }
        }

        for (int c = 0; c < maxCol; c++) res.put(c, c >= adjStart ? "ADJ" : "PHY");
        return res;
    }

    private Double parseNumeric(Cell cell, DataFormatter formatter) {
        if (cell == null) return null;
        if (cell.getCellType() == CellType.NUMERIC) return cell.getNumericCellValue();
        String raw = formatter.formatCellValue(cell);
        if (raw == null || raw.isBlank()) return null;
        String s = raw.replace(",", "").replace("%", "").trim();
        try {
            double v = Double.parseDouble(s);
            // 注意：你 schema 里 GR_* 的单位是 %，前端也按 % 画；这里不除以100，保持“3.98 => 3.98”
            return v;
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private String extractBizDate(String source) {
        String normalized = DateUtil.normalizeDate(source);
        if (normalized != null) return normalized;
        Matcher m = DATE_PATTERN.matcher(source);
        if (!m.find()) return null;
        return DateUtil.normalizeDate(m.group(1) + "-" + m.group(2) + "-" + m.group(3));
    }

    private String extractBizDateFromSheet(Sheet sheet) {
        DataFormatter formatter = new DataFormatter();
        // 常见位置：B2
        try {
            Row r = sheet.getRow(1);
            if (r != null) {
                String t = formatter.formatCellValue(r.getCell(1)).trim();
                String d = DateUtil.normalizeDate(t);
                if (d != null) return d;
            }
        } catch (Exception ignored) {}

        // 兜底：前 8 行扫日期
        for (int r = 0; r <= Math.min(8, sheet.getLastRowNum()); r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;
            for (int c = 0; c < row.getLastCellNum(); c++) {
                String t = formatter.formatCellValue(row.getCell(c)).trim();
                String d = DateUtil.normalizeDate(t);
                if (d != null) return d;
            }
        }
        return null;
    }
}
