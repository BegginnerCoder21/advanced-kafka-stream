package com.kafka.stream.greetings.launcher;

import com.kafka.stream.greetings.topology.ExploreJoinsOperatorsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.kafka.stream.greetings.topology.ExploreJoinsOperatorsTopology.ALPHABETS;
import static com.kafka.stream.greetings.topology.ExploreJoinsOperatorsTopology.ALPHABETS_ABBREVATIONS;

@Slf4j
public class JoiningStreamPlayGroundApp {


    public static void main(String[] args) {

      var kTableTopology = ExploreJoinsOperatorsTopology.build();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "joins1"); // consumer group
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000");

        createTopics(config, List.of(ALPHABETS,ALPHABETS_ABBREVATIONS ));

         var kafkaStreams = new KafkaStreams(kTableTopology, config);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        log.info("Starting Greeting streams");
        try {
            log.info("Demarrage du plan de traitement (topology)");
            kafkaStreams.start();
        } catch (Exception e) {
            log.info("Erreur lors du demarrage de la topology");
            throw new RuntimeException(e);
        }
    }

    private static void createTopicsCopartitioningDemo(Properties config, List<String> alphabets) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication = 1;

        var newTopics = alphabets
                .stream()
                .map(topic -> {
                    if (topic.equals(ALPHABETS_ABBREVATIONS)) {
                        return new NewTopic(topic, 3, replication);
                    }
                    return new NewTopic(topic, partitions, replication);
                }).toList();

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ", e.getMessage(), e);
        }

    }

    public static void createTopics(Properties config, List<String> topicNames) {
        try (AdminClient admin = AdminClient.create(config)) {
            int partitions = 1;
            short replication = 1;

            var newTopics = topicNames.stream()
                    .map(name -> new NewTopic(name, partitions, replication))
                    .toList();

            var createTopicResult = admin.createTopics(newTopics);

            createTopicResult.all().get();
            log.info("Topics créés avec succès.");

        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.warn("Le topic existe déjà. Pas de création nécessaire.");
                return;
            }

            log.error("Erreur inattendue lors de la création des topics.", e);
            throw new RuntimeException(e);

        } catch (Exception e) {
            log.error("Erreur lors de la création des topics.", e);
            throw new RuntimeException(e);
        }
    }


}