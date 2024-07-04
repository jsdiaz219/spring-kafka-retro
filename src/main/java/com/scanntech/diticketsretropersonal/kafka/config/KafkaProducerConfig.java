package com.scanntech.diticketsretropersonal.kafka.config;

import com.scanntech.diticketsretropersonal.avro.MovementAv;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerConfig {
    @Bean
    public ProducerFactory<Integer, MovementAv> producerFactory() {
        return new DefaultKafkaProducerFactory<>(senderProps());
    }

    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MovementAv.class);
        return props;
    }

    @Bean
    public KafkaTemplate<Integer, MovementAv> kafkaTemplate(ProducerFactory<Integer, MovementAv> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
