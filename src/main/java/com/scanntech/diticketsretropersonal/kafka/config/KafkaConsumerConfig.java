package com.scanntech.diticketsretropersonal.kafka.config;

import com.scanntech.di.commons.kafka.avro.model.TransaccionPendienteAv;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final KafkaConsumerConfigData configData;

    public KafkaConsumerConfig(KafkaConsumerConfigData configData) {
        this.configData = configData;
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TransaccionPendienteAv>>
    kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, TransaccionPendienteAv> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(configData.getConcurrency());
        factory.getContainerProperties().setPollTimeout(configData.getPollTimeout());
        factory.setBatchListener(configData.getBatchListener());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, TransaccionPendienteAv> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, configData.getMaxPollRecords());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.scanntech.di.commons.kafka.avro.deserializer.KafkaAvroTrnPendienteDeserializer");
        return props;
    }
}
