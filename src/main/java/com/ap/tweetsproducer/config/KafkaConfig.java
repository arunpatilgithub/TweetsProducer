package com.ap.tweetsproducer.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    public Properties getKafkaConnectionConfig() {

        Properties kafkaConnectionProperties = new Properties();

        kafkaConnectionProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                      "192.168.4.146:9092");

        kafkaConnectionProperties.put("schema.registry.url",
                                      "http://192.168.4.146:8081");

        kafkaConnectionProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                      StringSerializer.class);

        kafkaConnectionProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                      KafkaAvroSerializer.class);


        return kafkaConnectionProperties;
    }
}
