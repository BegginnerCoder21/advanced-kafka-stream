package com.kafka.stream.greetings.launcher;

import com.kafka.stream.greetings.exception.StreamsDeserializationExceptionHandler;
import com.kafka.stream.greetings.exception.StreamsProcessorCustomErrorHandler;
import com.kafka.stream.greetings.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamsDeserializationExceptionHandler.class);
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);


        createTopic(properties, List.of(GreetingsTopology.GREETINGS, GreetingsTopology.GREETINGS_UPPERCASE, GreetingsTopology.GREETINGS_SPANISH));

        var greetingsTopology = GreetingsTopology.buildTopology();

        var kafkaStreams = new KafkaStreams(greetingsTopology, properties);
        kafkaStreams.setUncaughtExceptionHandler(new StreamsProcessorCustomErrorHandler());

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
