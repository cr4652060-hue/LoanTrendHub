package com.example.loantrendhub.service;

import com.example.loantrendhub.model.FactRow;
import com.example.loantrendhub.repo.FactRepo;
import com.example.loantrendhub.util.BranchNormalizeUtil;
import com.example.loantrendhub.util.DateUtil;
import com.example.loantrendhub.util.ExcelUtil.HeaderResolver;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class IngestService {
    private static final Logger log = LoggerFactory.getLogger(IngestService.class);
    private static final Pattern DATE_PATTERN = Pattern.compile("(20\\d{2})[.-/](\\d{1,2})[.-/](\\d{1,2})");
    private static final Set<String> INVALID_BRANCH = Set.of(
            "\u5355\u4f4d", "\u5408\u8ba1", "\u5404\u9879\u8d37\u6b3e", "\u603b\u8ba1", "\u5236\u8868", "\u8bf4\u660e", "\u5c0f\u8ba1"
    );
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

        Map<String, String> aliasRawMap = factRepo.findBranchAliasRawMap();
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
                    messages.add("[SKIP] " + source + ": no biz date found");
                    continue;
                }

                ParseResult parsed = parseSheet(sheet, bizDate, source, aliasRawMap, aliasNormMap, canonicalBranches);
                int fileSaved = persistFileInChunks(parsed.rows());
                Map<String, Long> byBizDate = factRepo.countRowsBySourceFileByBizDate(source);
                Map<String, Long> byScope = factRepo.countRowsBySourceFileByScope(source);
                Map<String, Long> byMetric = factRepo.countRowsBySourceFileByMetric(source);
                totalRows += parsed.rows().size();
                totalSaved += fileSaved;
                totalUnknown += parsed.unknownCount();
                accepted.add(source);
                messages.add("[OK] " + source + ": date=" + bizDate + " rows=" + parsed.rows().size() + " saved=" + fileSaved + " reject=" + parsed.unknownCount());
                messages.add("[STAT] " + source + " rowsByBizDate=" + byBizDate);
                messages.add("[STAT] " + source + " rowsByScope=" + byScope);
                messages.add("[STAT] " + source + " rowsByMetric=" + byMetric);
                messages.addAll(parsed.warnings());
                log.info("import summary source={} rows={} saved={} reject={} rowsByBizDate={} rowsByScope={} rowsByMetric={}",
                        source, parsed.rows().size(), fileSaved, parsed.unknownCount(), byBizDate, byScope, byMetric);
            } catch (Exception ex) {
                Throwable root = rootCause(ex);
                String reason = root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
                log.error("ingest file failed: {}", source, ex);
                messages.add("[ERR] " + source + ": " + reason);
                throw new IllegalStateException("Import failed (" + source + "): " + reason, ex);
            }
        }

        return Map.of(
                "acceptedFiles", accepted,
                "rows", totalRows,
                "saved", totalSaved,
                "unknown", totalUnknown,
                "messages", messages
        );
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

    private ParseResult parseSheet(
            Sheet sheet,
            String bizDate,
            String sourceFile,
            Map<String, String> aliasRawMap,
            Map<String, String> aliasNormMap,
            Set<String> canonicalBranches
    ) {
        DataFormatter formatter = new DataFormatter();

        int maxCol = 0;
        for (Row row : sheet) {
            if (row != null && row.getLastCellNum() > maxCol) {
                maxCol = row.getLastCellNum();
            }
        }

        int headerDepth = detectHeaderDepth(sheet, formatter);
        int branchCol = detectBranchCol(sheet, formatter, headerDepth);
        Map<Integer, String> scopeByCol = detectScopeByColumn(sheet, maxCol, formatter);

        Map<Integer, String> metricByCol = new HashMap<>();
        for (int c = 0; c < maxCol; c++) {
            StringBuilder header = new StringBuilder();
            for (int r = 0; r <= headerDepth; r++) {
                Row row = sheet.getRow(r);
                if (row == null) {
                    continue;
                }
                String txt = formatter.formatCellValue(row.getCell(c));
                if (!txt.isBlank()) {
                    header.append(' ').append(txt.trim());
                }
            }
            String metric = HeaderResolver.resolveMetric(header.toString());
            if (metric != null) {
                metricByCol.put(c, metric);
            }
        }

        int dataStart = headerDepth + 1;
        List<FactRow> rows = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        int unknownCount = 0;

        for (int r = dataStart; r <= sheet.getLastRowNum(); r++) {
            Row row = sheet.getRow(r);
            if (row == null) {
                continue;
            }

            String rawBranch = formatter.formatCellValue(row.getCell(branchCol));
            BranchNormalizeUtil.BranchNormalized normalized = BranchNormalizeUtil.normalize(rawBranch);
            String displayBranch = normalized.displayCandidate();
            String normKey = normalized.normKey();
            if (displayBranch.isBlank()) {
                continue;
            }
            if (INVALID_BRANCH.stream().anyMatch(displayBranch::contains)) {
                continue;
            }

            ResolveBranchResult resolved = resolveBranch(rawBranch, displayBranch, normKey, aliasRawMap, aliasNormMap, canonicalBranches);
            if (resolved.rejectReason() != null) {
                unknownCount++;
                String reason = resolved.rejectReason();
                factRepo.logImportReject(sourceFile, r + 1, rawBranch == null ? "" : rawBranch, normKey, reason);
                warnings.add("[WARN] file=" + sourceFile + " row=" + (r + 1) + " raw='" + rawBranch + "' norm='" + normKey + "' reason=" + reason + " fallback='" + resolved.canonBranch() + "'");
            }
            if (resolved.canonBranch() == null || resolved.canonBranch().isBlank()) {
                continue;
            }

            for (Map.Entry<Integer, String> entry : metricByCol.entrySet()) {
                int c = entry.getKey();
                String scope = DateUtil.normalizeScope(scopeByCol.getOrDefault(c, "PHY"));
                Double val = parseNumeric(row.getCell(c), formatter);
                if (val == null) {
                    continue;
                }
                rows.add(new FactRow(bizDate, scope, resolved.canonBranch(), entry.getValue(), val, sourceFile, rawBranch, normKey));
            }
        }

        if (rows.isEmpty() && unknownCount == 0) {
            throw new IllegalArgumentException("Parsed result is empty. Please check header and branch column.");
        }
        return new ParseResult(rows, unknownCount, warnings);
    }

    private ResolveBranchResult resolveBranch(
            String rawBranch,
            String displayBranch,
            String normKey,
            Map<String, String> aliasRawMap,
            Map<String, String> aliasNormMap,
            Set<String> canonicalBranches
    ) {
        String trimmedRaw = rawBranch == null ? "" : rawBranch.trim();
        String canonical = aliasRawMap.get(displayBranch);
        if (canonical == null && !trimmedRaw.isBlank()) {
            canonical = aliasRawMap.get(trimmedRaw);
        }
        if (canonical == null && !normKey.isBlank()) {
            canonical = aliasNormMap.get(normKey);
        }
        if (canonical == null && canonicalBranches.contains(displayBranch)) {
            canonical = displayBranch;
        }
        if (canonical == null) {
            return new ResolveBranchResult(displayBranch, "no_alias_mapping");
        }
        if (!canonicalBranches.contains(canonical)) {
            return new ResolveBranchResult(canonical, "canon_branch_not_enabled:" + canonical);
        }
        return new ResolveBranchResult(canonical, null);
    }

    private int detectHeaderDepth(Sheet sheet, DataFormatter formatter) {
        int max = Math.min(12, sheet.getLastRowNum());
        int depth = 5;
        for (int r = 0; r <= max; r++) {
            Row row = sheet.getRow(r);
            if (row == null) {
                continue;
            }
            String line = rowToText(row, formatter).replace(" ", "");
            if (line.contains("\u5b9e\u4f53\u8d37\u6b3e")
                    || line.contains("\u7eaf\u8d26\u9762")
                    || line.contains("\u8fd8\u539f")
                    || line.contains("\u8f83\u4e0a\u65e5")
                    || line.contains("\u8f83\u4e0a\u6708")
                    || line.contains("\u8f83\u5e74\u521d")
                    || line.contains("\u589e\u91cf\u8f83\u540c\u671f")
                    || line.contains("\u589e\u5e45")
                    || line.contains("\u6237\u6570")
                    || line.contains("\u4f59\u989d")
                    || line.contains("\u5355\u4f4d")) {
                depth = r;
            }
        }
        return depth;
    }

    private int detectBranchCol(Sheet sheet, DataFormatter formatter, int headerDepth) {
        int maxCol = 0;
        for (int r = 0; r <= headerDepth; r++) {
            Row row = sheet.getRow(r);
            if (row != null && row.getLastCellNum() > maxCol) {
                maxCol = row.getLastCellNum();
            }
        }

        for (int r = 0; r <= headerDepth; r++) {
            Row row = sheet.getRow(r);
            if (row == null) {
                continue;
            }
            for (int c = 0; c < Math.max(maxCol, row.getLastCellNum()); c++) {
                String t = formatter.formatCellValue(row.getCell(c)).trim();
                if ("\u7f51\u70b9".equals(t) || t.contains("\u5355\u4f4d")) {
                    return c;
                }
            }
        }

        int dataStart = headerDepth + 1;
        int sampleEnd = Math.min(sheet.getLastRowNum(), dataStart + 10);
        int[] scores = new int[Math.max(maxCol, 2)];

        for (int r = dataStart; r <= sampleEnd; r++) {
            Row row = sheet.getRow(r);
            if (row == null) {
                continue;
            }
            for (int c = 0; c < scores.length; c++) {
                String text = formatter.formatCellValue(row.getCell(c)).trim();
                if (!text.isBlank() && looksLikeBranch(text)) {
                    scores[c]++;
                }
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
        if (s.length() < 2) {
            return false;
        }
        return s.contains("\u652f\u884c")
                || s.contains("\u5206\u7406\u5904")
                || s.contains("\u8425\u4e1a\u90e8")
                || s.contains("\u4fe1\u7528\u793e")
                || !s.matches(".*\\d.*");
    }

    private String rowToText(Row row, DataFormatter formatter) {
        StringBuilder sb = new StringBuilder();
        short last = row.getLastCellNum();
        for (int c = 0; c < last; c++) {
            String t = formatter.formatCellValue(row.getCell(c));
            if (!t.isBlank()) {
                sb.append(t).append(' ');
            }
        }
        return sb.toString();
    }

    private Map<Integer, String> detectScopeByColumn(Sheet sheet, int maxCol, DataFormatter formatter) {
        Map<Integer, String> res = new HashMap<>();
        int adjStart = maxCol / 2;

        for (int r = 0; r <= Math.min(8, sheet.getLastRowNum()); r++) {
            Row row = sheet.getRow(r);
            if (row == null) {
                continue;
            }
            for (int c = 0; c < maxCol; c++) {
                String t = formatter.formatCellValue(row.getCell(c)).replace(" ", "");
                if (t.contains("\u8fd8\u539f") || t.contains("\u5265\u8f6c") || t.contains("\u6838\u9500")) {
                    adjStart = Math.min(adjStart, c);
                }
            }
        }

        for (int c = 0; c < maxCol; c++) {
            res.put(c, c >= adjStart ? "ADJ" : "PHY");
        }
        return res;
    }

    private Double parseNumeric(Cell cell, DataFormatter formatter) {
        if (cell == null) {
            return null;
        }
        if (cell.getCellType() == CellType.NUMERIC) {
            return cell.getNumericCellValue();
        }
        String raw = formatter.formatCellValue(cell);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String s = raw.replace(",", "").replace("%", "").trim();
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private String extractBizDate(String source) {
        String normalized = DateUtil.normalizeDate(source);
        if (normalized != null) {
            return normalized;
        }
        Matcher m = DATE_PATTERN.matcher(source);
        if (!m.find()) {
            return null;
        }
        return DateUtil.normalizeDate(m.group(1) + "-" + m.group(2) + "-" + m.group(3));
    }

    private String extractBizDateFromSheet(Sheet sheet) {
        DataFormatter formatter = new DataFormatter();
        try {
            Row r = sheet.getRow(1);
            if (r != null) {
                String t = formatter.formatCellValue(r.getCell(1)).trim();
                String d = DateUtil.normalizeDate(t);
                if (d != null) {
                    return d;
                }
            }
        } catch (Exception ignored) {
        }

        for (int r = 0; r <= Math.min(8, sheet.getLastRowNum()); r++) {
            Row row = sheet.getRow(r);
            if (row == null) {
                continue;
            }
            for (int c = 0; c < row.getLastCellNum(); c++) {
                String t = formatter.formatCellValue(row.getCell(c)).trim();
                String d = DateUtil.normalizeDate(t);
                if (d != null) {
                    return d;
                }
            }
        }
        return null;
    }

    private record ParseResult(List<FactRow> rows, int unknownCount, List<String> warnings) {
    }

    private record ResolveBranchResult(String canonBranch, String rejectReason) {
    }
}
