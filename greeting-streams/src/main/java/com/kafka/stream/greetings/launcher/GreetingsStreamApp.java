package com.kafka.stream.greetings.launcher;

import com.kafka.stream.greetings.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;

import static java.util.stream.Collectors.toList;
//kafka-topics --bootstrap-server localhost:9092 --delete --topic greetings
//kafka-topics --bootstrap-server localhost:9092 --delete --topic greetings_uppercase
//kafka-topics --bootstrap-server localhost:9092 --delete --topic greetings_spanish

@Slf4j
public class GreetingsStreamApp {

    public static void main(String[] args){

        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);


        createTopic(properties, List.of(GreetingsTopology.GREETINGS, GreetingsTopology.GREETINGS_UPPERCASE, GreetingsTopology.GREETINGS_SPANISH));

        var greetingsTopology = GreetingsTopology.buildTopology();

        var kafkaStreams = new KafkaStreams(greetingsTopology, properties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        try {
            log.info("Demarrage du plan de traitement (topology)");
            kafkaStreams.start();
        } catch (Exception e) {
            log.info("Erreur lors du demarrage de la topology");
            throw new RuntimeException(e);
        }
    }

    private static void createTopic(Properties config, List<String> greetings)
    {
        AdminClient admin = AdminClient.create(config);
        var partitions = 2;
        short replication = 1;

        var newTopics = greetings.stream().map(topic -> new NewTopic(topic, partitions, replication)).toList();

        var createTopicResult = admin.createTopics(newTopics);

        try {
            createTopicResult.all().get();
            log.info("Topics crée avec succès.");
        } catch (Exception e) {
            log.error("Création de topic echoué.");
            throw new RuntimeException(e);
        }
    }
}
