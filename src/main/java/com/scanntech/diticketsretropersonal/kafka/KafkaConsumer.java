package com.scanntech.diticketsretropersonal.kafka;

import com.scanntech.di.commons.kafka.avro.model.TransaccionPendienteAv;
import com.scanntech.diticketsretropersonal.service.TicketsRetroService;
import org.apache.logging.log4j.LogManager;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
public class KafkaConsumer {
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(KafkaConsumer.class);

    private final TicketsRetroService service;

    public KafkaConsumer(TicketsRetroService service) {
        this.service = service;
    }

    @KafkaListener(id = "grupo", groupId = "grupo", topics = "${kafka.retro-topic:trn-tickets-retro}",
            containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Payload List<TransaccionPendienteAv> data) throws IOException {
//        List<TransaccionPendienteAv> filtered = data.stream().filter(
//                tr -> tr.getObjetoApi() instanceof MovimientoAv
//        ).toList();
//        log.info(filtered.toString());
        log.info("About to save %d movements".formatted(data.size()));
        this.service.saveNewMovements(data);
    }
}
