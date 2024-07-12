package com.scanntech.diticketsretropersonal.kafka;

import com.scanntech.di.commons.kafka.avro.model.TransaccionPendienteAv;
import com.scanntech.diticketsretropersonal.dto.MovementStatus;
import com.scanntech.diticketsretropersonal.repository.IMovementsRepo;
import com.scanntech.diticketsretropersonal.util.MovementsUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
public class CustomProducerListener implements ProducerListener<String, TransaccionPendienteAv> {

    private static final Logger log = LogManager.getLogger(CustomProducerListener.class);
    private final IMovementsRepo repo;

    public CustomProducerListener(IMovementsRepo repo) {
        this.repo = repo;
    }

    @Override
    public void onSuccess(ProducerRecord<String, TransaccionPendienteAv> producerRecord, RecordMetadata recordMetadata) {
        log.info("Producer record sent successfully: {}", producerRecord);
        try {
            this.saveToStatus(MovementStatus.PROCESSED, producerRecord);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onError(ProducerRecord<String, TransaccionPendienteAv> producerRecord, @Nullable RecordMetadata recordMetadata, Exception exception) {
        try {
            log.error("Producer record failed to send: {}", exception.getMessage());
            this.saveToStatus(MovementStatus.ERROR, producerRecord);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void saveToStatus(MovementStatus status, ProducerRecord<String, TransaccionPendienteAv> producerRecord) throws IOException {
        TransaccionPendienteAv movementAv = producerRecord.value();
        String key = MovementsUtil.generateMovementKey(movementAv, status);
        repo.saveManyMovementsToFile(key, List.of(movementAv));
    }
}
