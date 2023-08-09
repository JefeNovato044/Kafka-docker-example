package org.kafka.streams;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.kafka.streams.utils.LegoDataRefinery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ExampleStream {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ExampleStream.class);
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "MyFirstStream-3");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //broker1:29092,broker2:29093,broker3:29094
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ProducerConfig.ACKS_CONFIG, "all");


        String inputTopic = "my-topic";
        String  outputTopic = "ouput-topic";//modificar a topico correcto
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(inputTopic);

        String fileName = "/home/jefenovato/curso-kafka/kafka_examples/src/main/resources/lego_sets_clean.csv";
        try {
            LegoDataRefinery refinery = new LegoDataRefinery(fileName);

            source.map((key, value) ->
            {
                String newValue = refinery.refine(value);
                return  new KeyValue<String, String>(key, newValue);
            }).to(outputTopic);

            KafkaStreams streams = new KafkaStreams(builder.build(), config);
            streams.start();

            Runtime.getRuntime().addShutdownHook( new Thread(
                    () -> {
                        System.out.println("Shutting Down Stream");
                        streams.close();
                    }
            ));


        } catch (IOException e) {
            System.err.println("Error initializing LegoDataRefinery: " + e.getMessage());
            System.exit(1);
        }
    }
}
