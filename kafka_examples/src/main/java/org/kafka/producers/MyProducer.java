package org.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.course.utils.CsvReader;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class MyProducer {
    List<String> set_ids;
    //List <String



    public static void main(String[] args) throws IOException {
        String brokers = "localhost:9092";
        String outputTopic = "my-topic";

        //Propiedades para configurar el producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        //Crear producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Instance of random class
        Random rand = new Random();
        CsvReader reader = new CsvReader();
        String fileName = "/home/jefenovato/curso-kafka/kafka_examples/src/main/resources/id_price.csv";
        reader.read(fileName);

        // Generar 100 records
        for (int i = 0; i < 100; i++) {
            int random = rand.nextInt(reader.getCsvLength());
            float comision = rand.nextFloat();
            Map<String, String> row = reader.getRowById(random);
            float salePrice = Float.valueOf(row.get("list_price"))*(1 + comision);

            //Json string
            String jsonString = "{\"set_id\": " + Integer.parseInt(row.get("prod_id")) + "," + "\"price\": " +  salePrice + "}";
            //Crear record
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, ts.toString()+"-" + i, jsonString);
            //mandar record
            producer.send(record);
        }

        producer.close();

    }
}
