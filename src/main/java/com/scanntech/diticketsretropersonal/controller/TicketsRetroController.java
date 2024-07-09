package com.scanntech.diticketsretropersonal.controller;

import com.scanntech.diticketsretropersonal.avro.MovementAv;
import com.scanntech.diticketsretropersonal.dto.ReprocessDataDto;
import com.scanntech.diticketsretropersonal.kafka.KafkaProducerService;
import com.scanntech.diticketsretropersonal.service.TicketsRetroService;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/tickets-retro")
public class TicketsRetroController {
    private final TicketsRetroService service;
    private final KafkaProducerService producer;

    public TicketsRetroController(TicketsRetroService service, KafkaProducerService producer) {
        this.service = service;
        this.producer = producer;
    }

    @PostMapping("/reprocess")
    public String reprocess(@RequestBody ReprocessDataDto dto) throws IOException {
        this.service.reprocess(dto);
        return "Done";
    }

    @GetMapping
    public String testMethod() throws IOException {
        MovementAv build = MovementAv.newBuilder()
                .setStore(1)
                .setCompany(2)
                .setCommercialDate("10/10/1997")
                .build();
        this.producer.sendMessage(build);

        return "Done";
    }
}
