package com.kafka.stream.greetings.domain;

import java.math.BigDecimal;

public record Revenue(String locationId,
                      BigDecimal finalAmount) {
}
