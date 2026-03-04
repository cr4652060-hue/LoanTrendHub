package com.example.loantrendhub.controller;

import com.example.loantrendhub.service.ImportJobService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class IngestController {
    private final ImportJobService importJobService;

    public IngestController(ImportJobService importJobService) {
        this.importJobService = importJobService;
    }

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, Object> upload(@RequestPart(name = "files") List<MultipartFile> files) throws Exception {
        return importJobService.submit(files);
    }

    @GetMapping("/job/{jobId}")
    public Map<String, Object> job(@PathVariable("jobId") String jobId) {
        return importJobService.get(jobId);
    }
}