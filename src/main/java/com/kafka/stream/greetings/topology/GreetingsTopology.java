package com.kafka.stream.greetings.topology;

import com.kafka.stream.greetings.domain.Greetings;
import com.kafka.stream.greetings.serdes.GreetingSerdes;
import com.kafka.stream.greetings.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;

@Slf4j
public class GreetingsTopology {

    private GreetingsTopology(){}

    public static final String GREETINGS = "greetings";
    public static final String GREETINGS_UPPERCASE = "greetings_uppercase";
    public static final String GREETINGS_SPANISH = "greetings_spanish";

    public static Topology buildTopology()
    {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var mergeStream = getCustomStringKStream(streamsBuilder);

        mergeStream.print(Printed.<String, Greetings>toSysOut().withLabel("mergeStream"));

        var modifiedStream = mergeStream
                //.map((key, value) -> new KeyValue<>(key.toUpperCase(), value.toUpperCase()))
//                .filter((key, value) -> value.length() > 10)
//                .peek((key, value) -> log.info("after filter: key {}, value : {}", key, value))
                .mapValues((readonlyKey, value) -> new Greetings(value.message().toUpperCase(), value.timeStamp()))
//                .peek((key, value) -> {
//                    key.toUpperCase();
//                    log.info("after mapValues: key {}, value : {}", key, value);
//                })
//                .flatMapValues((readonlyKey, value) -> {
//                    var newValue = Arrays.asList(value.split(""));
//
//                    return newValue.stream().map(String::toUpperCase).toList();
//                })
//                .flatMap((key, value) -> {
//                    var newValue = Arrays.asList(value.split(""));
//                    return newValue.stream().map(v -> KeyValue.pair(v.toUpperCase(), v)).toList();
//                });
        ;

        modifiedStream.print(Printed.<String, Greetings>toSysOut().withLabel("modifiedStream"));

        modifiedStream.to(GREETINGS_UPPERCASE
                , Produced.with(Serdes.String(), SerdesFactory.GreetingSerdes())
        );

        return streamsBuilder.build();
    }

    private static KStream<String, String> getStringStringKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> greetingsStream = streamsBuilder.stream(GREETINGS
//                , Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> spanishStream = streamsBuilder.stream(GREETINGS_SPANISH
//                , Consumed.with(Serdes.String(), Serdes.String())
        );

        var mergeStream = greetingsStream.merge(spanishStream);
        return mergeStream;
    }

    private static KStream<String, Greetings> getCustomStringKStream(StreamsBuilder streamsBuilder) {
        KStream<String, Greetings> greetingsStream = streamsBuilder.stream(GREETINGS
                , Consumed.with(Serdes.String(), SerdesFactory.GreetingSerdes())
        );

        KStream<String, Greetings> spanishStream = streamsBuilder.stream(GREETINGS_SPANISH
                , Consumed.with(Serdes.String(), SerdesFactory.GreetingSerdes())
        );

        return greetingsStream.merge(spanishStream);
    }
}
