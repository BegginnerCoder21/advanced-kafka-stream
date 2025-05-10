package com.kafka.stream.greetings.serdes;

import com.kafka.stream.greetings.domain.Greetings;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public static Serde<Greetings> GreetingSerdes() {
        return new GreetingSerdes();
    }
}
