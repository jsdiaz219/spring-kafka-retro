package com.scanntech.diticketsretropersonal.kafka;

import com.scanntech.diticketsretropersonal.avro.MovementAv;
import com.scanntech.diticketsretropersonal.service.TicketsRetroService;
import org.apache.logging.log4j.LogManager;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Component
public class KafkaConsumer {
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(KafkaConsumer.class);

    private final TicketsRetroService service;

    public KafkaConsumer(TicketsRetroService service) {
        this.service = service;
    }

    @KafkaListener(id = "grupo", groupId = "grupo", topics = "trn-tickets-retro",
            containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Payload List<MovementAv> data) {
//      TODO - ON real code, we should filter the movements over daily closings
//        transaccionPendienteAv.getObjetoApi() instanceof MovimientoAv
        List<MovementAv> filtered = data.stream().filter(Objects::nonNull).toList();
        log.info(filtered.toString());
        log.info("About to save %d movements".formatted(filtered.size()));
        this.service.saveNewMovements(filtered);
    }
}
