package com.scanntech.diticketsretropersonal.kafka.config;

import com.scanntech.di.commons.kafka.avro.model.TransaccionPendienteAv;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaProducerConfig {
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.scanntech.di.commons.kafka.avro.serializer.KafkaAvroTrnPendienteSerializer");
        return props;
    }

    @Bean
    public ProducerFactory<String, TransaccionPendienteAv> producerFactory() {
        return new DefaultKafkaProducerFactory<>(senderProps());
    }

    @Bean
    public KafkaTemplate<String, TransaccionPendienteAv> kafkaTemplate(ProducerFactory<String, TransaccionPendienteAv> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
