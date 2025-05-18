package com.kafka.stream.greetings.launcher;
import com.kafka.stream.greetings.topology.ExploreKTableTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;

import static com.kafka.stream.greetings.launcher.GreetingsStreamApp.createTopic;
import static com.kafka.stream.greetings.producer.WordsProducer.WORDS;

@Slf4j
public class KTableStreamApp {

    public static void main(String[] args){

        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "Ktable");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");


        createTopic(properties, List.of(WORDS));

        var kTableTopology = ExploreKTableTopology.build();

        var kafkaStreams = new KafkaStreams(kTableTopology, properties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        try {
            log.info("Demarrage du plan de traitement (topology)");
            kafkaStreams.start();
        } catch (Exception e) {
            log.info("Erreur lors du demarrage de la topology");
            throw new RuntimeException(e);
        }
    }
}
