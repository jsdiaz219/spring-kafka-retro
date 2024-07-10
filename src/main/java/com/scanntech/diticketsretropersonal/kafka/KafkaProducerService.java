package com.scanntech.diticketsretropersonal.kafka;

import com.scanntech.di.commons.kafka.avro.model.TransaccionPendienteAv;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Service
public class KafkaProducerService {


    private final String REPROCESS_TOPIC;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public KafkaProducerService(@Value("${kafka.reprocess-topic:trn-tickets-retro-reprocess}") String reprocessTopic,
                                KafkaTemplate<String, byte[]> kafkaTemplate,
                                CustomProducerListener customProducerListener) {
        this.REPROCESS_TOPIC = reprocessTopic;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaTemplate.setProducerListener(customProducerListener);
    }

    public void sendMessage(TransaccionPendienteAv message) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        SpecificDatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(message.getSchema());
        writer.write(message, encoder);
        encoder.flush();
        outputStream.close();

        kafkaTemplate.send(new ProducerRecord<>(REPROCESS_TOPIC, outputStream.toByteArray()));
    }
}

