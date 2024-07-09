package com.scanntech.diticketsretropersonal.service;

import com.scanntech.diticketsretropersonal.avro.MovementAv;
import com.scanntech.diticketsretropersonal.dto.MovementStatus;
import com.scanntech.diticketsretropersonal.dto.ReprocessDataDto;
import com.scanntech.diticketsretropersonal.kafka.KafkaProducerService;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.regex.Matcher;

@Service
public class TicketsRetroService {

    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(TicketsRetroService.class);

    private static final String dir = System.getProperty("user.dir");

    private final String baseDir;

    private final KafkaProducerService producer;

    public TicketsRetroService(@Value("${movements.base-dir:NONE}") String baseDir, KafkaProducerService producer) {
        this.baseDir = baseDir.equals("NONE") ? dir + "/local-data/" : baseDir;
        this.producer = producer;
    }

    public void reprocess(ReprocessDataDto reprocessDataDto) throws IOException {
        String pathRegex = reprocessDataDto.toRegexString(MovementStatus.PENDING);
        log.info("Looking for files with regex: %s".formatted(pathRegex));
        List<Path> foundFiles = findFiles(this.baseDir, pathRegex);
        log.info("Found %d files".formatted(foundFiles.size()));

        List<MovementAv> movements = new ArrayList<>();
        for (Path path: foundFiles) {
            File file = path.toFile();
            movements.addAll(readMovementsFromFile(file));
        }

        this.publishAllMovements(movements);
    }

    private void publishAllMovements(List<MovementAv> movements) {
        for (MovementAv movement: movements) {
            try {
                this.producer.sendMessage(movement);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<MovementAv> readMovementsFromFile(File file) throws IOException {
        List<MovementAv> movements = new ArrayList<>();
        DatumReader<MovementAv> userDatumReader = new SpecificDatumReader<>(MovementAv.class);
        try (DataFileReader<MovementAv> dataFileReader = new DataFileReader<>(file, userDatumReader)) {
            MovementAv movementAv = null;
            while (dataFileReader.hasNext()) {
                movementAv = dataFileReader.next(movementAv);
                movements.add(movementAv);
            }
        }

        return movements;
    }


    public static List<Path> findFiles(String baseDir, String pattern) throws IOException {
        List<Path> result = new ArrayList<>();
        Path basePath = Paths.get(baseDir);
        PathMatcher matcher = createPathMatcher(pattern);

        Files.walkFileTree(basePath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                Path relativePath = basePath.relativize(file);
                if (matcher.matches(relativePath)) {
                    result.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        return result;
    }

    private static PathMatcher createPathMatcher(String pattern) {
        // Reemplaza los '*' con el patrón adecuado de regex
        String regexPattern = pattern
                .replace("**", ".*")
                .replace("*", "[^/]*");

        // Agrega los delimitadores de inicio y fin de la cadena
        regexPattern = "^" + regexPattern + "$";

        Pattern compiledPattern = Pattern.compile(regexPattern);

        return path -> {
            Matcher matcher = compiledPattern.matcher(path.toString());
            return matcher.matches();
        };
    }

    public void saveNewMovements(List<MovementAv> movements) {
        try {
            Map<String, List<MovementAv>> groupedMovements = groupMovementsByKey(movements);

            for (Map.Entry<String, List<MovementAv>> entry: groupedMovements.entrySet()) {
                log.info(entry.getValue().toString());
                writeToFile(entry.getValue(), baseDir + entry.getKey());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkParentDirectories(String filePath) {
        Path path = Paths.get(filePath);
        Path parent = path.getParent();

        if (parent != null) {
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void writeToFile(List<MovementAv> movements, String filePath) throws IOException {
        checkParentDirectories(filePath);
        DatumWriter<MovementAv> movementDatumWriter = new SpecificDatumWriter<>(MovementAv.class);

        File file = new File(filePath);
        DataFileWriter<MovementAv> dataFileWriter;

        if (file.exists()) {
            dataFileWriter = new DataFileWriter<>(movementDatumWriter);
            dataFileWriter.appendTo(file);
        } else {
            dataFileWriter = new DataFileWriter<>(movementDatumWriter);
            dataFileWriter.create(movements.get(0).getSchema(), file);
        }

        for (MovementAv movementAv: movements) {
            try {
                dataFileWriter.append(movementAv);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        dataFileWriter.close();
    }

    private Map<String, List<MovementAv>> groupMovementsByKey(List<MovementAv> movements) {
        return movements.stream().collect(Collectors.groupingBy(this::generateMovementKey));
    }

    private String generateMovementKey(MovementAv movementAv) {
        String[] dateSplit = movementAv.getCommercialDate().toString().split("/");
        String month = dateSplit[1];
        String year = dateSplit[2];
        String company = String.valueOf(movementAv.getCompany());
        String store = String.valueOf(movementAv.getStore());

        return "%s-%s/%s/%s/%s.avro".formatted(month, year, company, MovementStatus.PENDING, store);
    }
}
