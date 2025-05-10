package com.kafka.stream.greetings.domain;

import java.time.LocalDateTime;

public record Greetings(String message, LocalDateTime timeStamp) {
}
