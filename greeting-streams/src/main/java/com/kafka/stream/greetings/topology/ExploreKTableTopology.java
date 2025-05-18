package com.kafka.stream.greetings.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

import static com.kafka.stream.greetings.producer.WordsProducer.WORDS;

public class ExploreKTableTopology {

    private ExploreKTableTopology(){}

    public static Topology build()
    {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<String, String> wordsTable = streamsBuilder
                .table(WORDS, Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("words-store"));

        wordsTable.filter((key, value) -> value.length() > 2)
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("words-ktable"));

        return streamsBuilder.build();
    }
}
