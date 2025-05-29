package com.kafka.stream.greetings.topology;

import com.kafka.stream.greetings.serdes.SerdesFactory;
import com.kafka.stream.greetings.utils.AlphabetWordAggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class AggregateOperatorTopology {

    private AggregateOperatorTopology(){}

    public static final String AGGREGATE = "aggregate";
    public static Topology aggregateBuilder()
    {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder.stream(AGGREGATE, Consumed.with(Serdes.String(), Serdes.String()));

        inputStream.print(Printed.<String, String>toSysOut().withLabel(AGGREGATE));

//        KGroupedStream<String, String> stringStringKGroupedStream = inputStream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        KGroupedStream<String, String> stringKGroupedStream = inputStream.groupByKey((Grouped.with(Serdes.String(), Serdes.String())));

        exploreCount(stringKGroupedStream);
        exploreReduce(stringKGroupedStream);
//        exploreAggregate(stringKGroupedStream);

        return streamsBuilder.build();
    }

    private static void exploreAggregate(KGroupedStream<String, String> aggregateKGroupedStream) {

        Initializer<AlphabetWordAggregate> initializer = AlphabetWordAggregate::new;

        Aggregator<String, String, AlphabetWordAggregate> aggregator = (key, value, aggregate) -> aggregate.updateNewEvent(key, value);

        KTable<String, AlphabetWordAggregate> aggregate = aggregateKGroupedStream
                .aggregate(initializer, aggregator, Materialized
                        .<String, AlphabetWordAggregate, KeyValueStore<Bytes, byte[]>>as("aggregate-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SerdesFactory.aggregateSerdes()));

        aggregate
                .toStream()
                .print(Printed.<String, AlphabetWordAggregate>toSysOut()
                        .withLabel("aggregated-words"));
    }

    private static void exploreReduce(KGroupedStream<String, String> reduceKGroupedStream) {
        
        KTable<String, String> reduce = reduceKGroupedStream.reduce((value1, value2) -> {
            log.info("value1 : {}, value2: {}", value1, value2);
            return value1.toUpperCase() + "-" + value2.toUpperCase();
        },
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce-words")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                );

        reduce
                .toStream()
                .print(Printed.<String, String>toSysOut()
                        .withLabel("reduce-words"));
    }

    private static void exploreCount(KGroupedStream<String, String> stringKGroupedStream) {

        KTable<String, Long> count = stringKGroupedStream
                .count(Named.as("count-per-alphabet"),
                        Materialized.as("count-per-alphabet")
                        );

        count
                .toStream()
                .print(Printed.<String, Long>toSysOut()
                        .withLabel("words-count-per-alphabet"));
    }


}
