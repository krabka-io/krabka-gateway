package dev.krabka.sdk;

public record RecordResult(int partition, long offset, boolean deduplicated) {}
