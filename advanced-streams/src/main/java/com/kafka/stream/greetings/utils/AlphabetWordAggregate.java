package com.kafka.stream.greetings.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;

@Slf4j
public record AlphabetWordAggregate(String key, HashSet<String> valueList, int runningCount) {

    public AlphabetWordAggregate(){
        this("", new HashSet<>(), 0);
    }

    public AlphabetWordAggregate updateNewEvent(String key, String value)
    {
        log.info("New Record Key: {}, Value: {}", key, value);

        int newRunningCount = this.runningCount + 1;
        this.valueList.add(value);

        AlphabetWordAggregate alphabetWordAggregate = new AlphabetWordAggregate(key, this.valueList, newRunningCount);
        log.info("aggregateValue: {}", alphabetWordAggregate);

        return alphabetWordAggregate;
    }
}
