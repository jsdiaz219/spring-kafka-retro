package com.scanntech.diticketsretropersonal.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.scanntech.di.commons.kafka.avro.model.TransaccionPendienteAv;
import com.scanntech.diticketsretropersonal.dto.MovementStatus;
import com.scanntech.diticketsretropersonal.dto.ReprocessDataDto;
import com.scanntech.diticketsretropersonal.kafka.KafkaProducerService;
import com.scanntech.diticketsretropersonal.repository.IMovementsRepo;
import com.scanntech.diticketsretropersonal.util.MovementsUtil;
import org.apache.logging.log4j.LogManager;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class TicketsRetroService {

    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(TicketsRetroService.class);

    private final KafkaProducerService producer;
    private final IMovementsRepo repo;

    public TicketsRetroService(KafkaProducerService producer, IMovementsRepo repo) {
        this.producer = producer;
        this.repo = repo;
    }

    public void reprocess(ReprocessDataDto reprocessDataDto) throws IOException {
        String pathRegex = reprocessDataDto.toRegexString();
        log.info("Looking for files with regex: %s".formatted(pathRegex));
       List<TransaccionPendienteAv> movements = this.repo.findMovements(pathRegex);

        this.publishAllMovements(movements);
    }

    private void publishAllMovements(List<TransaccionPendienteAv> movements) {
        for (TransaccionPendienteAv movement: movements) {
            try {
                this.producer.sendMessage(movement);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void saveNewMovements(List<TransaccionPendienteAv> movements) throws IOException {
        Map<String, List<TransaccionPendienteAv>> groupedMovements = groupMovementsByKey(movements);

        for (Map.Entry<String, List<TransaccionPendienteAv>> entry: groupedMovements.entrySet()) {
            this.repo.saveManyMovementsToFile(entry.getKey(), entry.getValue());
        }
    }

    private Map<String, List<TransaccionPendienteAv>> groupMovementsByKey(List<TransaccionPendienteAv> movements) {
        return movements.stream().collect(Collectors.groupingBy(
                movementAv -> {
                    try {
                        return MovementsUtil.generateMovementKey(movementAv, MovementStatus.PENDING);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
        ));
    }
}
