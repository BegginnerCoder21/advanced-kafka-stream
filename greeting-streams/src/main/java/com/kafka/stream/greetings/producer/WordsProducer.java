package com.kafka.stream.greetings.producer;

import lombok.extern.slf4j.Slf4j;

import static com.kafka.stream.greetings.utils.ProducerUtil.publishMessageSync;

@Slf4j
public class WordsProducer {

    public static final String WORDS = "words";

    public static void main(String[] args) throws InterruptedException {

        var key = "A";

        var word = "Apple";
        var word1 = "Alligator";
        var word2 = "Ambulance5";

        var recordMetaData = publishMessageSync(WORDS, key,word);
        log.info("Published the alphabet message 1 : {} ", recordMetaData);

        var recordMetaData1 = publishMessageSync(WORDS, key,word1);
        log.info("Published the alphabet message 2 : {} ", recordMetaData1);

        var recordMetaData2 = publishMessageSync(WORDS, key,word2);
        log.info("Published the alphabet message 3 : {} ", recordMetaData2);

        var bKey = "B";

        var bWord1 = "Bus";
        var bWord2 = "Baby";
        var recordMetaData3 = publishMessageSync(WORDS, bKey,bWord1);
        log.info("Published the alphabet message B1 : {} ", recordMetaData2);

        var recordMetaData4 = publishMessageSync(WORDS, bKey,bWord2);
        log.info("Published the alphabet message B2: {} ", recordMetaData2);

    }
}
