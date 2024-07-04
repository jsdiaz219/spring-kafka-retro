package com.scanntech.diticketsretropersonal.kafka;

import com.scanntech.diticketsretropersonal.avro.MovementAv;
import com.scanntech.diticketsretropersonal.service.TicketsRetroService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Component
public class KafkaConsumer {
    public static Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

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
