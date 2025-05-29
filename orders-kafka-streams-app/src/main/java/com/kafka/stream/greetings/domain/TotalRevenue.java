package com.kafka.stream.greetings.domain;

import java.math.BigDecimal;

public record TotalRevenue(String locationId, Integer runningOrderCount, BigDecimal runningOrderRevenue) {

    public TotalRevenue()
    {
        this("", 0, BigDecimal.ZERO);
    }

    public TotalRevenue updateRunningRevenue(String key, Order order) {

        Integer newRunningCount = this.runningOrderCount + 1;
        BigDecimal newFinalAmount = this.runningOrderRevenue.add(order.finalAmount());

        return new TotalRevenue(key, newRunningCount, newFinalAmount);
    }
}
