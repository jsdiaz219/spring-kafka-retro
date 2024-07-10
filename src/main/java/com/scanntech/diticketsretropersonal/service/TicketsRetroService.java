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

//    private List<MovementAv> readMovementsFromFile(File file) throws IOException {
//        List<MovementAv> movements = new ArrayList<>();
//        DatumReader<MovementAv> userDatumReader = new SpecificDatumReader<>(MovementAv.class);
//        try (DataFileReader<MovementAv> dataFileReader = new DataFileReader<>(file, userDatumReader)) {
//            MovementAv movementAv = null;
//            while (dataFileReader.hasNext()) {
//                movementAv = dataFileReader.next(movementAv);
//                movements.add(movementAv);
//            }
//        }
//
//        return movements;
//    }


//    public static List<Path> findFiles(String baseDir, String pattern) throws IOException {
//        List<Path> result = new ArrayList<>();
//        Path basePath = Paths.get(baseDir);
//        PathMatcher matcher = createPathMatcher(pattern);
//
//        Files.walkFileTree(basePath, new SimpleFileVisitor<>() {
//            @Override
//            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
//                Path relativePath = basePath.relativize(file);
//                if (matcher.matches(relativePath)) {
//                    result.add(file);
//                }
//                return FileVisitResult.CONTINUE;
//            }
//        });
//
//        return result;
//    }

//    private static PathMatcher createPathMatcher(String pattern) {
//        // Reemplaza los '*' con el patrón adecuado de regex
//        String regexPattern = pattern
//                .replace("**", ".*")
//                .replace("*", "[^/]*");
//
//        // Agrega los delimitadores de inicio y fin de la cadena
//        regexPattern = "^" + regexPattern + "$";
//
//        Pattern compiledPattern = Pattern.compile(regexPattern);
//
//        return path -> {
//            Matcher matcher = compiledPattern.matcher(path.toString());
//            return matcher.matches();
//        };
//    }

    public void saveNewMovements(List<TransaccionPendienteAv> movements) throws IOException {
        Map<String, List<TransaccionPendienteAv>> groupedMovements = groupMovementsByKey(movements);

        for (Map.Entry<String, List<TransaccionPendienteAv>> entry: groupedMovements.entrySet()) {
            log.info(entry.getValue().toString());
            this.repo.saveManyMovementsToFile(entry.getKey(), entry.getValue());
        }
    }

//    private void checkParentDirectories(String filePath) {
//        Path path = Paths.get(filePath);
//        Path parent = path.getParent();
//
//        if (parent != null) {
//            try {
//                Files.createDirectories(parent);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }

//    private void writeToFile(List<MovementAv> movements, String filePath) throws IOException {
//        DatumWriter<MovementAv> movementDatumWriter = new SpecificDatumWriter<>(MovementAv.class);
//
//        File file = new File(filePath);
//        DataFileWriter<MovementAv> dataFileWriter;
//
//        if (file.exists()) {
//            dataFileWriter = new DataFileWriter<>(movementDatumWriter);
//            dataFileWriter.appendTo(file);
//        } else {
//            dataFileWriter = new DataFileWriter<>(movementDatumWriter);
//            dataFileWriter.create(movements.get(0).getSchema(), file);
//        }
//
//        for (MovementAv movementAv: movements) {
//            try {
//                dataFileWriter.append(movementAv);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//        dataFileWriter.close();
//    }

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
