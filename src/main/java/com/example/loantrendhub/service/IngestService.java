package com.example.loantrendhub.service;

import com.example.loantrendhub.model.FactRow;
import com.example.loantrendhub.repo.FactRepo;
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
    private final FactRepo factRepo;

    public IngestService(FactRepo factRepo) {
        this.factRepo = factRepo;
    }

    public Map<String, Object> ingest(List<MultipartFile> files) throws Exception {
        List<FactRow> all = new ArrayList<>();
        List<String> accepted = new ArrayList<>();
        for (MultipartFile file : files) {
            if (file == null || file.isEmpty()) continue;
            String source = file.getOriginalFilename() == null ? "unknown.xlsx" : file.getOriginalFilename();
            String bizDate = extractBizDate(source);
            try (InputStream in = file.getInputStream(); Workbook wb = WorkbookFactory.create(in)) {
                Sheet sheet = wb.getSheetAt(0);
                if (bizDate == null) bizDate = extractBizDateFromSheet(sheet);
                if (bizDate == null) continue;
                all.addAll(parseSheet(sheet, bizDate, source));
                accepted.add(source);
            }
        }
        int saved = all.isEmpty() ? 0 : Arrays.stream(factRepo.upsertBatch(all)).sum();
        return Map.of("acceptedFiles", accepted, "rows", all.size(), "saved", saved);
    }

    private List<FactRow> parseSheet(Sheet sheet, String bizDate, String sourceFile) {
        DataFormatter formatter = new DataFormatter();
        int maxCol = 0;
        for (Row row : sheet) if (row.getLastCellNum() > maxCol) maxCol = row.getLastCellNum();

        Map<Integer, String> scopeByCol = detectScopeByColumn(sheet, maxCol, formatter);
        int headerDepth = 6;
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

        List<FactRow> rows = new ArrayList<>();
        for (int r = headerDepth + 1; r <= sheet.getLastRowNum(); r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;
            String branch = formatter.formatCellValue(row.getCell(0)).trim();
            if (branch.isBlank() || branch.contains("各项贷款") || branch.contains("单位")) continue;

            for (Map.Entry<Integer, String> e : metricByCol.entrySet()) {
                int c = e.getKey();
                String scope = scopeByCol.getOrDefault(c, "PHY");
                Double value = parseNumeric(row.getCell(c), formatter);
                if (value == null) continue;
                rows.add(new FactRow(bizDate, scope, branch, e.getValue(), value, sourceFile));
            }
        }
        return rows;
    }

    private Map<Integer, String> detectScopeByColumn(Sheet sheet, int maxCol, DataFormatter formatter) {
        Map<Integer, String> res = new HashMap<>();
        int pureStart = 1;
        int adjStart = maxCol / 2;

        for (int r = 0; r <= Math.min(6, sheet.getLastRowNum()); r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;
            for (int c = 0; c < maxCol; c++) {
                String t = formatter.formatCellValue(row.getCell(c)).replace(" ", "");
                if (t.contains("纯账面")) pureStart = c;
                if (t.contains("还原") || t.contains("剔转") || t.contains("核销")) adjStart = c;
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
            return raw.contains("%") ? v / 100.0 : v;
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
        for (int r = 0; r <= Math.min(5, sheet.getLastRowNum()); r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;
            for (int c = 0; c < Math.max(20, row.getLastCellNum()); c++) {
                String text = formatter.formatCellValue(row.getCell(c));
                String normalized = DateUtil.normalizeDate(text);
                if (normalized != null) return normalized;
            }
        }
        return null;
    }
}