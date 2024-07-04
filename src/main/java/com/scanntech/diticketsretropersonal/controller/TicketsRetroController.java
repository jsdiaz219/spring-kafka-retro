package com.scanntech.diticketsretropersonal.controller;

import com.scanntech.diticketsretropersonal.avro.MovementAv;
import com.scanntech.diticketsretropersonal.dto.ReprocessDataDto;
import com.scanntech.diticketsretropersonal.service.TicketsRetroService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@RestController
@RequestMapping("/tickets-retro")
public class TicketsRetroController {
    private final TicketsRetroService service;
    private final KafkaTemplate<String, MovementAv> kafkaTemplate;

    public TicketsRetroController(TicketsRetroService service, KafkaTemplate<String, MovementAv> kafkaTemplate) {
        this.service = service;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/reprocess")
    public List<Path> reprocess(@RequestBody ReprocessDataDto dto) throws IOException {
        return this.service.reprocess(dto);
    }

    @GetMapping
    public void testMethod() {
        MovementAv build = MovementAv.newBuilder().build();
        build.setStore(1);
        build.setCompany(2);
        build.setCommercialDate("10/10/1997");
        this.kafkaTemplate.send("trn-tickets-retro-reprocess", build);
    }
}
