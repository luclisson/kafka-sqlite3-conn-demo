package de.coba.kafka.model;

import java.time.Instant;

public record DemoMessage(String text, Instant createdAt) {
}
