package com.scanntech.diticketsretropersonal.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scanntech.di.commons.kafka.avro.model.MovimientoAv;
import com.scanntech.di.commons.kafka.avro.model.TransaccionPendienteAv;
import com.scanntech.diticketsretropersonal.dto.MovementStatus;
import io.micrometer.core.instrument.util.StringUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class MovementsUtil {
    public static String TIME_SEPARATOR = "T";
    public static String SPACE_SEPARATOR = " ";

    public static String generateMovementKey(TransaccionPendienteAv movement, MovementStatus status) throws JsonProcessingException {
        MovimientoAv transactionMovement = (MovimientoAv) movement.getObjetoApi();
        LocalDate commercialDate = getDateFromString(transactionMovement.getFecha().toString());
        assert commercialDate != null;
        String month = String.valueOf(commercialDate.getMonth().getValue());
        String year = String.valueOf(commercialDate.getMonth().getValue());
        String company = String.valueOf(movement.getEmpresa());
        String jsonbArgumentsString = movement.getJsonbArguments().toString();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonbArguments = mapper.readTree(jsonbArgumentsString);
        String store = jsonbArguments.get("P2").toString();

        return "%s-%s/%s/%s/%s.avro".formatted(month, year, company, status.getValue(), store);
    }

    public static LocalDate getDateFromString(String stringDate){
        if(!StringUtils.isEmpty(stringDate)){
            if (stringDate.contains(TIME_SEPARATOR)) {
                stringDate = stringDate.substring(0, stringDate.indexOf(TIME_SEPARATOR));
            } else if (stringDate.contains(SPACE_SEPARATOR)) {
                stringDate = stringDate.substring(0, stringDate.indexOf(SPACE_SEPARATOR));
            }

            return LocalDate.parse(
                    stringDate,
                    DateTimeFormatter.ofPattern("yyyy-M-d")
            );
        } else {
            return null;
        }
    }
}
