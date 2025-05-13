package com.kafka.stream.greetings.serdes;

import com.kafka.stream.greetings.domain.Order;
import com.kafka.stream.greetings.domain.Revenue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {
    private SerdesFactory(){}

    public static Serde<Order> orderSerdesUsingGeneric() {
        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Order> jsonDeserializer = new JsonDeserializer<>(Order.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    public static Serde<Revenue> revenueSerdesUsingGeneric() {
        JsonSerializer<Revenue> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Revenue> jsonDeserializer = new JsonDeserializer<>(Revenue.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
