package com.kafka.stream.greetings.producer;

import com.kafka.stream.greetings.utils.ProducerUtil;
import lombok.extern.slf4j.Slf4j;
import java.util.Map;

import static com.kafka.stream.greetings.topology.ExploreJoinsOperatorsTopology.ALPHABETS;
import static com.kafka.stream.greetings.topology.ExploreJoinsOperatorsTopology.ALPHABETS_ABBREVATIONS;
import static com.kafka.stream.greetings.utils.ProducerUtil.publishMessageSync;
import static com.kafka.stream.greetings.utils.ProducerUtil.publishMessageSyncWithDelay;

@Slf4j
public class JoinsMockDataProducer {


    public static void main(String[] args) {


        var alphabetMap = Map.of(
//                "A", "A is the first letter in English Alphabets.",
//                "B", "B is the second letter in English Alphabets."
                //              ,"E", "E is the fifth letter in English Alphabets."
//                ,
                "A", "A is the First letter in English Alphabets.",
                "B", "B is the Second letter in English Alphabets."
        );
        publishMessages(alphabetMap, ALPHABETS);

        //-4 & 4 will trigger the join
        //-6 -5 & 5, 6 wont trigger the join
        //publishMessagesWithDelay(alphabetMap, ALPHABETS, 5);

        var alphabetAbbrevationMap = Map.of(
                "A", "Apple",
                "B", "Bus"
                , "C", "Cat"

        );
        publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVATIONS);

        alphabetAbbrevationMap = Map.of(
                "A", "Airplane",
                "B", "Baby."

        );
        // publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVATIONS);

    }

    private static void publishMessagesWithDelay(Map<String, String> alphabetMap, String topic, int delaySeconds) {
        alphabetMap
                .forEach((key, value) -> {
                    var recordMetaData = publishMessageSyncWithDelay(topic, key, value, delaySeconds);
                    log.info("Published the alphabet message : {} ", recordMetaData);
                });
    }


    private static void publishMessages(Map<String, String> alphabetMap, String topic) {

        alphabetMap
                .forEach((key, value) -> {
                    var recordMetaData = publishMessageSync(topic, key, value);
                    log.info("Published the alphabet message : {} ", recordMetaData);
                });
    }


}