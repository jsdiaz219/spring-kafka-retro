package com.scanntech.diticketsretropersonal.kafka;

import com.scanntech.di.commons.kafka.avro.model.TransaccionPendienteAv;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class KafkaProducerService {


    private final String REPROCESS_TOPIC;

    private final KafkaTemplate<String, TransaccionPendienteAv> kafkaTemplate;

    public KafkaProducerService(@Value("${kafka.reprocess-topic:trn-tickets-retro-reprocess}") String reprocessTopic,
                                KafkaTemplate<String, TransaccionPendienteAv> kafkaTemplate,
                                CustomProducerListener customProducerListener) {
        this.REPROCESS_TOPIC = reprocessTopic;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTemplate.setProducerListener(customProducerListener);
    }

    public void sendMessage(TransaccionPendienteAv message) throws IOException {
        kafkaTemplate.send(new ProducerRecord<>(REPROCESS_TOPIC, String.valueOf(message.getChecksum()), message));
    }
}

