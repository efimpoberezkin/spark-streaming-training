package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class TopicGenerator implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    public static void main(String[] args) throws IOException {
        // load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final String sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);

            try (Producer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
                 BufferedReader reader = new BufferedReader(new FileReader(sampleFile))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    MonitoringRecord record = new MonitoringRecord(line.split(","));
                    ProducerRecord<String, MonitoringRecord> message
                            = new ProducerRecord<>(topicName, record);
                    producer.send(message);
                }
            }
        }
    }
}
