package com.kafka.stream.greetings.topology;

import com.kafka.stream.greetings.domain.Alphabet;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class ExploreJoinsOperatorsTopology {

    public static final String ALPHABETS = "alphabets";
    public static final String ALPHABETS_ABBREVATIONS = "alphabets_abbreviation";
    public static Topology build()
    {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //joinKStreamWithKTable(streamsBuilder);
        joinKStreamWithGlobalKTable(streamsBuilder);

        return streamsBuilder.build();
    }

    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder) {

        KStream<String, String> alphabetsAbbreviation = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbreviation
                .print(Printed.<String, String>toSysOut()
                        .withLabel(ALPHABETS_ABBREVATIONS));

        KTable<String, String> alphabetsTable = streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as(ALPHABETS_ABBREVATIONS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KStream<String, Alphabet> joinResult = alphabetsAbbreviation.join(alphabetsTable, valueJoiner);

        joinResult
                .print(Printed.<String, Alphabet>toSysOut()
                        .withLabel("alphabets-with-abbreviations"));

    }

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {

        KStream<String, String> alphabetsAbbreviation = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS,
                        Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbreviation
                .print(Printed.<String, String>toSysOut()
                        .withLabel(ALPHABETS_ABBREVATIONS));

        GlobalKTable<String, String> alphabetsGlobalKTable = streamsBuilder
                .globalTable(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as(ALPHABETS_ABBREVATIONS));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KeyValueMapper<String, String, String> keyValueMapper = (leftKey, rightValue) -> leftKey;
        KStream<String, Alphabet> joinResult = alphabetsAbbreviation.join(alphabetsGlobalKTable,keyValueMapper, valueJoiner);

        joinResult
                .print(Printed.<String, Alphabet>toSysOut()
                        .withLabel("alphabets-with-abbreviations"));

    }
}
