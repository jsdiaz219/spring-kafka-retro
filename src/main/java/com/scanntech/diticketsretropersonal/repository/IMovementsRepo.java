package com.scanntech.diticketsretropersonal.repository;

import com.scanntech.di.commons.kafka.avro.model.TransaccionPendienteAv;

import java.io.IOException;
import java.util.List;

public interface IMovementsRepo {
    void saveManyMovementsToFile(String path, List<TransaccionPendienteAv> movements) throws IOException;
    List<TransaccionPendienteAv> findMovements(String pathRegex) throws IOException;
}
