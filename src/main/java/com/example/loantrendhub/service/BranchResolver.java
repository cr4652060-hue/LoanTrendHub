package com.example.loantrendhub.service;

import com.example.loantrendhub.repo.FactRepo;
import com.example.loantrendhub.util.BranchNormalizeUtil;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Service
public class BranchResolver {
    private final FactRepo factRepo;

    public BranchResolver(FactRepo factRepo) {
        this.factRepo = factRepo;
    }

    public String resolve(String rawBranch) {
        BranchNormalizeUtil.BranchNormalized normalized = BranchNormalizeUtil.normalize(rawBranch);
        if (normalized.displayCandidate().isBlank() || normalized.normKey().isBlank()) {
            return null;
        }
        Set<String> canonical = new HashSet<>(factRepo.findAllBranches());
        if (canonical.contains(normalized.displayCandidate())) {
            return normalized.displayCandidate();
        }
        Map<String, String> aliasByNorm = factRepo.findBranchAliasNormMap();
        return aliasByNorm.get(normalized.normKey());
    }
}