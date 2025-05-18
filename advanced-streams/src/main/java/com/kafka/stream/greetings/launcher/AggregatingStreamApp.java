package com.kafka.stream.greetings.launcher;

import com.kafka.stream.greetings.topology.AggregateOperatorTopology;
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


import static com.kafka.stream.greetings.topology.AggregateOperatorTopology.AGGREGATE;

@Slf4j
public class AggregatingStreamApp {

    public static void main(String[] args){

        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");

        createTopic(properties, List.of(AGGREGATE));

        var greetingsTopology = AggregateOperatorTopology.aggregateBuilder();

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

    public static void createTopic(Properties config, List<String> topicNames) {
        try (AdminClient admin = AdminClient.create(config)) {
            int partitions = 2;
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
