package com.example.loantrendhub.service;

import com.example.loantrendhub.model.FactRow;
import com.example.loantrendhub.repo.FactRepo;
import com.example.loantrendhub.util.BranchNormalizeUtil;
import com.example.loantrendhub.util.DateUtil;
import com.example.loantrendhub.util.ExcelUtil.HeaderResolver;
import org.apache.poi.ss.usermodel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class IngestService {
    private static final Logger log = LoggerFactory.getLogger(IngestService.class);
    private static final Pattern DATE_PATTERN = Pattern.compile("(20\\d{2})[.-/](\\d{1,2})[.-/](\\d{1,2})");
    private static final Set<String> INVALID_BRANCH = Set.of("单位", "合计", "各项贷款", "总计", "制表", "注");
    private static final int UPSERT_BATCH_SIZE = 1000;
    private final FactRepo factRepo;
    private final TransactionTemplate transactionTemplate;

    public IngestService(FactRepo factRepo, TransactionTemplate transactionTemplate) {
        this.factRepo = factRepo;
        this.transactionTemplate = transactionTemplate;
    }

    public Map<String, Object> ingest(List<MultipartFile> files) throws Exception {
        List<ImportJobService.StoredUpload> stored = new ArrayList<>();
        for (MultipartFile file : files) {
            if (file == null || file.isEmpty()) {
                continue;
            }
            String source = file.getOriginalFilename() == null ? "unknown.xlsx" : file.getOriginalFilename();
            java.nio.file.Path temp = Files.createTempFile("loantrend-upload-", ".xlsx");
            file.transferTo(temp);
            stored.add(new ImportJobService.StoredUpload(source, temp));
        }
        return ingestStored(stored);
    }

    public Map<String, Object> ingestStored(List<ImportJobService.StoredUpload> files) throws Exception {
        int totalRows = 0;
        int totalSaved = 0;
        int totalUnknown = 0;
        List<String> accepted = new ArrayList<>();
        List<String> messages = new ArrayList<>();
        Map<String, String> aliasNormMap = factRepo.findBranchAliasNormMap();
        Set<String> canonicalBranches = new HashSet<>(factRepo.findAllBranches());
        for (ImportJobService.StoredUpload file : files) {
            String source = file.sourceName();
            String bizDate = extractBizDate(source);

            try (InputStream in = Files.newInputStream(file.path()); Workbook wb = WorkbookFactory.create(in)) {
                Sheet sheet = wb.getSheetAt(0);
                if (bizDate == null) {
                    bizDate = extractBizDateFromSheet(sheet);
                }
                if (bizDate == null) {
                    messages.add("[SKIP] " + source + "：未识别到业务日期");
                    continue;
                }
                ParseResult parsed = parseSheet(sheet, bizDate, source, aliasNormMap, canonicalBranches);
                int fileSaved = persistFileInChunks(parsed.rows());
                totalRows += parsed.rows().size();
                totalSaved += fileSaved;
                totalUnknown += parsed.unknownCount();
                accepted.add(source);
                messages.add("[OK] " + source + "：date=" + bizDate + " rows=" + parsed.rows().size() + " saved=" + fileSaved + " unknown=" + parsed.unknownCount());
                messages.addAll(parsed.warnings());
            } catch (Exception ex) {
                Throwable root = rootCause(ex);
                String reason = root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
                log.error("ingest file failed: {}", source, ex);
                messages.add("[ERR] " + source + "：" + reason);
                throw new IllegalStateException("导入失败(" + source + "): " + reason, ex);
            }
        }

        return Map.of("acceptedFiles", accepted, "rows", totalRows, "saved", totalSaved, "unknown", totalUnknown, "messages", messages);
    }


    private int persistFileInChunks(List<FactRow> parsed) {
        return transactionTemplate.execute(status -> {
            int saved = 0;
            for (int i = 0; i < parsed.size(); i += UPSERT_BATCH_SIZE) {
                int end = Math.min(i + UPSERT_BATCH_SIZE, parsed.size());
                List<FactRow> chunk = parsed.subList(i, end);
                saved += Arrays.stream(factRepo.upsertBatch(chunk)).sum();
            }
            return saved;
        });
    }

    private Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private ParseResult parseSheet(Sheet sheet, String bizDate, String sourceFile, Map<String, String> aliasNormMap, Set<String> canonicalBranches) {
        DataFormatter formatter = new DataFormatter();

        int maxCol = 0;
        for (Row row : sheet) if (row != null && row.getLastCellNum() > maxCol) maxCol = row.getLastCellNum();

        int headerDepth = detectHeaderDepth(sheet, formatter);
        int branchCol = detectBranchCol(sheet, formatter, headerDepth);
        Map<Integer, String> scopeByCol = detectScopeByColumn(sheet, maxCol, formatter);

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
        List<String> warnings = new ArrayList<>();
        int unknownCount = 0;

        for (int r = dataStart; r <= sheet.getLastRowNum(); r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;

            String rawBranch = formatter.formatCellValue(row.getCell(branchCol));
            BranchNormalizeUtil.BranchNormalized normalized = BranchNormalizeUtil.normalize(rawBranch);
            String displayBranch = normalized.displayCandidate();
            String normKey = normalized.normKey();
            if (displayBranch.isBlank()) continue;
            if (INVALID_BRANCH.stream().anyMatch(displayBranch::contains)) continue;

            String branch = canonicalBranches.contains(displayBranch) ? displayBranch : aliasNormMap.get(normKey);
            if (branch == null || branch.isBlank()) {
                unknownCount++;
                factRepo.logUnknownBranch(rawBranch, normKey, sourceFile, r + 1);
                warnings.add("[WARN] unknown branch: file=" + sourceFile + " row=" + (r + 1) + " raw='" + rawBranch + "' norm='" + normKey + "'");
                continue;
            }

            for (Map.Entry<Integer, String> e : metricByCol.entrySet()) {
                int c = e.getKey();
                String scope = DateUtil.normalizeScope(scopeByCol.getOrDefault(c, "PHY"));
                Double val = parseNumeric(row.getCell(c), formatter);
                if (val == null) continue;
                rows.add(new FactRow(bizDate, scope, branch, e.getValue(), val, sourceFile, rawBranch, normKey));
            }
        }
        if (rows.isEmpty() && unknownCount == 0) {
            throw new IllegalArgumentException("解析结果为空：请确认表头未被改坏，且“单位/网点”列可识别（常见在B列）");
        }
        return new ParseResult(rows, unknownCount, warnings);
    }


    private int detectHeaderDepth(Sheet sheet, DataFormatter formatter) {
        int max = Math.min(12, sheet.getLastRowNum());
        int depth = 5;
        for (int r = 0; r <= max; r++) {
            Row row = sheet.getRow(r);
            if (row == null) continue;
            String line = rowToText(row, formatter).replace(" ", "");
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
                if ("网点".equals(t) || t.contains("单位")) return c;
            }
        }
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
        if (s.length() < 2) return false;
        return s.contains("支行") || s.contains("分理处") || s.contains("营业部") || s.contains("信用社") || !s.matches(".*\\d.*");
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
            return Double.parseDouble(s);
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
        try {
            Row r = sheet.getRow(1);
            if (r != null) {
                String t = formatter.formatCellValue(r.getCell(1)).trim();
                String d = DateUtil.normalizeDate(t);
                if (d != null) return d;
            }
        } catch (Exception ignored) {}

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

    private record ParseResult(List<FactRow> rows, int unknownCount, List<String> warnings) {}
}