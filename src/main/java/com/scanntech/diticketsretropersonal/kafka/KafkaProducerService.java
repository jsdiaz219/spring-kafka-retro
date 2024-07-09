package com.scanntech.diticketsretropersonal.kafka;

import com.scanntech.diticketsretropersonal.avro.MovementAv;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Service
public class KafkaProducerService {

    private static final String TOPIC = "trn-tickets-retro-reprocess";

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(MovementAv message) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        SpecificDatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(message.getSchema());
        writer.write(message, encoder);
        encoder.flush();
        outputStream.close();

        kafkaTemplate.send(new ProducerRecord<>(TOPIC, outputStream.toByteArray()));
    }
}

