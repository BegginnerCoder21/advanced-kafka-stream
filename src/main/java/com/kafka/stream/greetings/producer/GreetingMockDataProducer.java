package com.kafka.stream.greetings.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.stream.greetings.domain.Greetings;
import com.kafka.stream.greetings.topology.GreetingsTopology;
import com.kafka.stream.greetings.utils.ProducerUtil;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
public class GreetingMockDataProducer {

    public static void main(String[] args)
    {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        spanishGreetings(objectMapper);
        englishGreetings(objectMapper);

    }

    private static void spanishGreetings(ObjectMapper objectMapper)
    {
        var spanishGreeting = List.of(
                new Greetings("¡Hola buenos dias!", LocalDateTime.now()),
                new Greetings("¡Hola buenas tardes!", LocalDateTime.now()),
                new Greetings("¡Hola, buenas noches!", LocalDateTime.now())
        );

        producerMessage(objectMapper, spanishGreeting, GreetingsTopology.GREETINGS_SPANISH);
    }

    private static void englishGreetings(ObjectMapper objectMapper)
    {
        var spanishGreeting = List.of(
                new Greetings("Hello, Good Morning!", LocalDateTime.now()),
                new Greetings("Hello, Good Evening!", LocalDateTime.now()),
                new Greetings("Hello, Good Night!", LocalDateTime.now())
        );

        producerMessage(objectMapper, spanishGreeting, GreetingsTopology.GREETINGS);
    }

    private static void producerMessage(ObjectMapper objectMapper, List<Greetings> greetingList, String topicName) {
        greetingList.forEach((greetings -> {
            try {
                var greetingJson = objectMapper.writeValueAsString(greetings);
                var recordMetaData = ProducerUtil.publishMessageSync(topicName, null, greetingJson);
            } catch (JsonProcessingException e) {
                log.error("JsonProcessingException Error, Published the publishMessageSync alphabet message : {}", e.getMessage(), e);
                throw new RuntimeException(e);
            } catch (Exception e) {
                log.error("Exception Error Published the alphabet message : {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }));
    }
}
