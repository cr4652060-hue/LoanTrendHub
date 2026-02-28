package com.example.loantrendhub.model;

public record GrowthDef(
        String date,
        Double value,
        Double dodRate,
        Double mtdRate,
        Double ytdRate,
        String note
) {}