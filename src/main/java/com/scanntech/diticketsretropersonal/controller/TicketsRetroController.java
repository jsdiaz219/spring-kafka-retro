package com.scanntech.diticketsretropersonal.controller;

import com.scanntech.diticketsretropersonal.dto.ReprocessDataDto;
import com.scanntech.diticketsretropersonal.service.TicketsRetroService;
import javax.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/tickets-retro")
public class TicketsRetroController {
    private final TicketsRetroService service;

    public TicketsRetroController(TicketsRetroService service) {
        this.service = service;
    }

    @PostMapping("/reprocess")
    public String reprocess(@RequestBody @Valid ReprocessDataDto dto) throws IOException {
        this.service.reprocess(dto);
        return "Done";
    }
}
