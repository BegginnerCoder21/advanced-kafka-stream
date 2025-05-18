package com.kafka.stream.greetings.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class AggregateOperatorTopology {

    private AggregateOperatorTopology(){}

    public static final String AGGREGATE = "aggregate";
    public static Topology aggregateBuilder()
    {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder.stream(AGGREGATE, Consumed.with(Serdes.String(), Serdes.String()));

        inputStream.print(Printed.<String, String>toSysOut().withLabel(AGGREGATE));

        KGroupedStream<String, String> stringStringKGroupedStream = inputStream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        exploreCount(stringStringKGroupedStream);

        return streamsBuilder.build();
    }

    private static void exploreCount(KGroupedStream<String, String> stringStringKGroupedStream) {

        KTable<String, Long> count = stringStringKGroupedStream
                .count(Named.as("count-per-alphabet"));

        count
                .toStream()
                .print(Printed.<String, Long>toSysOut()
                        .withLabel("words-count-per-alphabet"));
    }
}
