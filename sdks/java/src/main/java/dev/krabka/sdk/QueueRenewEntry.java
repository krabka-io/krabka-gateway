package dev.krabka.sdk;

import java.util.Objects;

public record QueueRenewEntry(String messageId) {
    public QueueRenewEntry {
        Objects.requireNonNull(messageId, "messageId");
    }
}
