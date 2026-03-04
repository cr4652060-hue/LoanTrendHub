package com.example.loantrendhub.service;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

@Service
public class ImportJobService {
    private final ExecutorService importPool = Executors.newFixedThreadPool(2);
    private final Map<String, JobState> jobs = new ConcurrentHashMap<>();
    private final IngestService ingestService;

    public ImportJobService(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    public Map<String, Object> submit(List<MultipartFile> files) throws Exception {
        String jobId = UUID.randomUUID().toString();
        JobState state = JobState.pending();
        jobs.put(jobId, state);

        List<StoredUpload> stagedFiles = stage(files);
        state.totalFiles = stagedFiles.size();
        state.status = "RUNNING";

        importPool.submit(() -> run(jobId, state, stagedFiles));
        return Map.of("jobId", jobId, "status", state.status, "totalFiles", state.totalFiles);
    }

    public Map<String, Object> get(String jobId) {
        JobState state = jobs.get(jobId);
        if (state == null) {
            return Map.of("jobId", jobId, "status", "NOT_FOUND");
        }
        return state.toMap(jobId);
    }

    private void run(String jobId, JobState state, List<StoredUpload> stagedFiles) {
        try {
            Map<String, Object> result = ingestService.ingestStored(stagedFiles);
            state.status = "SUCCESS";
            state.completedAt = Instant.now().toString();
            state.result = result;
            state.doneFiles = stagedFiles.size();
        } catch (Exception ex) {
            state.status = "FAILED";
            state.completedAt = Instant.now().toString();
            state.error = ex.getMessage();
        } finally {
            for (StoredUpload file : stagedFiles) {
                try {
                    Files.deleteIfExists(file.path());
                } catch (Exception ignored) {
                }
            }
            jobs.put(jobId, state);
        }
    }

    private List<StoredUpload> stage(List<MultipartFile> files) throws Exception {
        Path baseDir = Path.of("tmp");
        Files.createDirectories(baseDir);
        List<StoredUpload> staged = new ArrayList<>();
        for (MultipartFile file : files) {
            if (file == null || file.isEmpty()) {
                continue;
            }
            String original = file.getOriginalFilename() == null ? "unknown.xlsx" : file.getOriginalFilename();
            Path path = baseDir.resolve(System.currentTimeMillis() + "_" + UUID.randomUUID() + "_" + original);
            Files.copy(file.getInputStream(), path, StandardCopyOption.REPLACE_EXISTING);
            staged.add(new StoredUpload(original, path));
        }
        return staged;
    }

    public record StoredUpload(String sourceName, Path path) {}

    private static final class JobState {
        String status;
        int totalFiles;
        int doneFiles;
        String createdAt;
        String completedAt;
        String error;
        Map<String, Object> result;

        static JobState pending() {
            JobState s = new JobState();
            s.status = "PENDING";
            s.createdAt = Instant.now().toString();
            s.totalFiles = 0;
            s.doneFiles = 0;
            return s;
        }

        Map<String, Object> toMap(String jobId) {
            return Map.of(
                    "jobId", jobId,
                    "status", status,
                    "createdAt", createdAt,
                    "completedAt", completedAt == null ? "" : completedAt,
                    "totalFiles", totalFiles,
                    "doneFiles", doneFiles,
                    "error", error == null ? "" : error,
                    "result", result == null ? Map.of() : result
            );
        }
    }
}